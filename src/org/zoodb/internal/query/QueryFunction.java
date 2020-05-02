/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal.query;

import static org.zoodb.internal.query.TypeConverterTools.convertToString;
import static org.zoodb.internal.query.TypeConverterTools.toDouble;
import static org.zoodb.internal.query.TypeConverterTools.toFloat;
import static org.zoodb.internal.query.TypeConverterTools.toInt;
import static org.zoodb.internal.query.TypeConverterTools.toLong;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryExecutor.VariableInstance;
import org.zoodb.internal.query.QueryOptimizerV4.IndexProposalSet;
import org.zoodb.internal.query.QueryParser.FNCT_OP;
import org.zoodb.internal.query.TypeConverterTools.COMPARISON_TYPE;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.jdo.impl.QueryImpl;

/**
 * Query functions.
 * 
 * @author ztilmann
 *
 * TODO Query optimization: 
 * - swap 'AND' operands such that the 'faster' branch is calculated first. We can then skip the 
 *   second branch if the first==false.  
 * - Aggregate constant LHS/RHS query functions into a single constant, for example for calculations 
 *   such as 1+2+3=x or _date=date+duration
 *   - One step further: rearrange constants to allow aggregation, for example:
 *     "const_date == _date+const_duration" TO "const_date-const_duration==_date" TO
 *     "const_date2 == _date".
 */
public class QueryFunction {

	private Class<?> returnType;
	private final ZooClassDef returnTypeDef;
	private final FNCT_OP fnct;
	private final int fieldId;
	private final Field field;
	private final ZooFieldDef zField;
	private final QueryFunction param0;
	private QueryFunction param1;
	private QueryFunction param2;
	private final Object constant;
	private final boolean isConstant; //indicate whether evaluation is constant
	private final boolean isDependentOnParameter; 
	@Deprecated
	private boolean isConstrained = false;
	private QueryAdvice qa;
	private QueryAdvice.Type qaType;
	
	private final COMPARISON_TYPE comparisonType;
	
	private QueryFunction(FNCT_OP fnct, ZooFieldDef zField, 
			Object constant, Class<?> returnType, ZooClassDef returnTypeDef) {
		this(fnct, zField, constant, returnType, returnTypeDef, null, null, null, null);
	}
	
	private QueryFunction(FNCT_OP fnct, ZooFieldDef zField, 
			Object constant, Class<?> returnType, ZooClassDef returnTypeDef, 
			QueryFunction param0) {
		this(fnct, zField, constant, returnType, returnTypeDef, null, param0, null, null);
	}
	
	private QueryFunction(FNCT_OP fnct, ZooFieldDef zField, 
			Object constant, Class<?> returnType, ZooClassDef returnTypeDef, 
			COMPARISON_TYPE comparisonType, QueryFunction param0, QueryFunction param1, QueryFunction param2) {
		this.returnType = returnType;
		this.returnTypeDef = returnTypeDef;
		this.fnct = fnct;
		this.zField = zField;
		if (zField != null) {
			fieldId = zField.getFieldPos();
			field = zField.getJavaField();
		} else {
			field = null;
			if (fnct == FNCT_OP.VARIABLE){
				fieldId = ((QueryVariable)constant).getId();
			} else if (fnct == FNCT_OP.THIS){
				fieldId = 0;
			} else {
				fieldId = -1;
			}
		}
		this.comparisonType = comparisonType;
		this.constant = constant;
		this.param0 = param0;
		this.param1 = param1;
		this.param2 = param2;
		this.isConstant = fnct == FNCT_OP.CONSTANT;
		this.isDependentOnParameter = 
				fnct == FNCT_OP.PARAM
				|| (param0 != null && param0.isDependentOnParameter)
				|| (param1 != null && param1.isDependentOnParameter)
				|| (param2 != null && param2.isDependentOnParameter);
	}
	
	/**
	 * 
	 * @return 'true' if this sub-tree yields a constant result, independent of parameters etc
	 */
	public boolean isConstant() {
		return isConstant;
	}

	public boolean isFixed() {
		return isConstant || fnct == FNCT_OP.PARAM;
	}

	public Class<?> getReturnType() {
		return returnType;
	}

	public void setReturnType(Class<?> returnType) {
		this.returnType = returnType;
	}

	public static QueryFunction createConstant(Object constant) {
		return new QueryFunction(FNCT_OP.CONSTANT, null, constant, constant.getClass(), null);
	}
	
	public static QueryFunction createThis(ZooClassDef baseClassDef) {
		return new QueryFunction(FNCT_OP.THIS, null, null, 
				baseClassDef.getJavaClass(), baseClassDef);
	}
	
	public static QueryFunction createFieldRef(QueryFunction baseObjectFn, ZooFieldDef field) {
		return new QueryFunction(FNCT_OP.REF, field, null, field.getJavaType(), 
				field.isPersistentType() ? field.getType() : null, baseObjectFn);
	}
	
	public static QueryFunction createFieldSCO(QueryFunction baseObjectFn, ZooFieldDef field) {
		return new QueryFunction(FNCT_OP.FIELD, field, null, field.getJavaType(), 
				null, baseObjectFn);
	}
	
	public static QueryFunction createJava(FNCT_OP op, QueryFunction[] params) {
		if (op == FNCT_OP.Math_abs) {
			return new QueryFunction(op, null, null, params[1].getReturnType(), null, null,  params[0], params[1], null);
		}
		switch (params.length) {
		case 0: return new QueryFunction(op, null, null, op.getReturnType(), null, null, null, null, null); 
		case 1: return new QueryFunction(op, null, null, op.getReturnType(), null, null, params[0], null, null); 
		case 2: return new QueryFunction(op, null, null, op.getReturnType(), null, null, params[0], params[1], null); 
		case 3: return new QueryFunction(op, null, null, op.getReturnType(), null, null, params[0], params[1], params[2]); 
		default: throw new UnsupportedOperationException(); 
		}
		
	}
	
	public static QueryFunction createJava(FNCT_OP op, QueryFunction param0, QueryFunction param1) {
		if (op == FNCT_OP.Math_abs) {
			//abs() can have four different return types, depending on the parameters!
			return new QueryFunction(op, null, null, param1.getReturnType(), null, null,  param0, param1, null);
		}
		QueryFunction fn = new QueryFunction(op, null, null, op.getReturnType(), null, null,  param0, param1, null);
		return simplifyConstantFn(fn);
	}
	
	public static QueryFunction createJava(FNCT_OP op, QueryFunction param0, QueryFunction param1, QueryFunction param2) {
		if (op == FNCT_OP.Math_abs) {
			//abs() can have four different return types, depending on the parameters!
			return new QueryFunction(op, null, null, param1.getReturnType(), null, null,  param0, param1, param2);
		}
		QueryFunction fn = new QueryFunction(op, null, null, op.getReturnType(), null, null,  param0, param1, param2);
		return simplifyConstantFn(fn);
	}
	
	public static QueryFunction createParam(ParameterDeclaration param) {
		return new QueryFunction(FNCT_OP.PARAM, null, param, param.getType(), param.getTypeDef());
	}
	
	public static QueryFunction createVariable(QueryVariable variable) {
		return new QueryFunction(FNCT_OP.VARIABLE, null, variable, variable.getType(), variable.getTypeDef());
	}
	
	public static QueryFunction createComparison(FNCT_OP op, boolean negate, QueryFunction fThis, QueryFunction fnL, QueryFunction fnR) {
		op = negate ? op.negate() : op;
		COMPARISON_TYPE comparisonType = TypeConverterTools.fromTypes(fnL.returnType, fnR.returnType);
		QueryFunction fn = new QueryFunction(op, null, null, op.getReturnType(), null, comparisonType, fThis, fnL, fnR);
		return simplifyConstantFn(fn);
	}
	
	private static QueryFunction simplifyConstantFn(QueryFunction fn) {
		if (fn.fnct == FNCT_OP.CONSTANT 
				|| fn.fnct == FNCT_OP.THIS 
				|| fn.fnct == FNCT_OP.FIELD
				|| fn.fnct == FNCT_OP.REF
				|| fn.fnct != FNCT_OP.PARAM) {
			return fn;
		}
		boolean isConstant = (fn.param0 == null || fn.param0.isConstant) 
				&& (fn.param1 == null || fn.param1.isConstant)
				&& (fn.param2 == null || fn.param2.isConstant);
		if (!isConstant) {
			return fn;
		}
		//calculate constant result
		Object constant = fn.evaluate(null, null, null, null);
		return createConstant(constant);
	}
	
	
	public Object evaluateWithIterator(Object candidate, VariableInstance[] vars, 
			Object[] executionParams) {
		return evaluate(candidate, candidate, vars, executionParams);
	}
	
	/**
	 * 
	 * @param currentInstance The current context for calling methods
	 * @param globalInstance The global context for 'this'
	 * @param vars Variables
	 * @return Result of the evaluation
	 */
	Object evaluate(Object currentInstance, Object globalInstance, VariableInstance[] vars,
			Object[] executionParams) {
		switch (fnct) {
		case REF:
		case FIELD:
			try {
				Object localInstance = param0.evaluate(currentInstance, globalInstance, vars, 
						executionParams);
				if (localInstance == QueryTerm.NULL || localInstance == QueryTerm.INVALID) {
					return QueryTerm.INVALID;
				}
				if (localInstance instanceof ZooPC) {
					if (((ZooPC)localInstance).jdoZooIsStateHollow()) {
						((ZooPC)localInstance).zooActivateRead();
					}
				}
				if (field.getDeclaringClass() == localInstance.getClass()) {
					//Shortcut for base class, optimization for non-polymorphism
					Object ret = field.get(localInstance);
					return ret == null ? QueryTerm.NULL : ret;
				}
				//TODO why don't we need this in QueryTerm.evaluate()????
				ZooClassDef def = ((ZooPC)localInstance).jdoZooGetClassDef();
				Object ret = def.getAllFields()[fieldId].getJavaField().get(localInstance);
				return ret == null ? QueryTerm.NULL : ret;
			} catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
				throw new RuntimeException(e);
			}
		case CONSTANT: return constant;
		case PARAM: return ((ParameterDeclaration)constant).getValue(executionParams);
		case VARIABLE: return vars[fieldId].value;
		case THIS: return globalInstance;
		default: return evaluateFunction(
				param0.evaluate(currentInstance, globalInstance, vars, executionParams),
				globalInstance, vars, executionParams);
		}
	}

	private Object getValue(Object localInstance, Object globalInstance, QueryFunction paramX,
			VariableInstance[] vars, Object[] executionParams) {
		if (paramX.op() == FNCT_OP.VARIABLE) {
			VariableInstance var = vars[paramX.fieldId];
			if (qaType != null) {
				//initialize variable
				//TODO
				throw new UnsupportedOperationException("Query is too complex. "
						+ "Please simplify query and/or avoid using variables.");
//				TODO
//				- implement 'nested' execution:
//					* Clear current iterator (not reset, we don;t need the first element yet)
//					* Increment parent iterator
//					* find out which part of the tree we need re-evaluating
//				
//				Should we maintain a STACK?
			}
			if (var.hasCollectionConstraint && var.iter() == null) {
				
				//TODO only do this if the Optimizer says so!
				//'null ' indicates that we need to initialize the Variable
				Iterator<?> iter;
				switch (fnct) {
				case COLL_contains:
					iter = ((Collection<?>)localInstance).iterator();
					break;
				case MAP_containsKey:
					iter = ((Map<?, ?>)localInstance).values().iterator();
					break;
				case MAP_containsValue:
					iter = ((Map<?, ?>)localInstance).values().iterator();
					break;
				case EQ:
					
				default: 
					throw new IllegalStateException(fnct.name());
				}
				if (iter.hasNext()) {
					var.setIterator(iter);
					var.value = iter.next();
				} else {
					return QueryTerm.INVALID;
				}
			} else if (vars[paramX.fieldId].value == null) {
				//We want to assign a variable here, this is only possible with `=`				
				if (fnct != FNCT_OP.EQ && fnct != FNCT_OP.EQ_BOOL) {
					throw new UnsupportedOperationException("Query is too complex. "
							+ "Variable can only be constrained with '='.");
				}
											
				//TODO evaluate var.identityConstraint instead of param1/2!!!!!
				//TODO evaluate var.identityConstraint instead of param1/2!!!!!
				//TODO evaluate var.identityConstraint instead of param1/2!!!!!
//				if (var.hasAdvices()) {
				//TODO is this correct???????/
//					QueryFunction ic = var.getAdvices().get(0).getIdentityConstraint();
//					if (ic != null) {
//						return ic.evaluateWithIterator(localInstance, vars);
//					}
//					//TODO remove, only for testing
//					throw new RuntimeException();
//				}
				
				//If iterator != null and value == null -> IdentityConstraint
				Object x;
				if (paramX == param1) {
					x = param2.evaluate(localInstance, globalInstance, vars, executionParams);
				} else {
					x = param1.evaluate(localInstance, globalInstance, vars, executionParams);
				}
				if (true) throw new UnsupportedOperationException("Query is too complex. "
						+ "Please simplify query and/or avoid using variables.");
				//((SingleIterator)vars[paramX.fieldId].iter).setValue(x);
				//TODO directly abort here with 'true'
				return x;
			}
		}
		Object x = paramX.evaluate(localInstance, globalInstance, vars, executionParams);
		//TODO add flag to enum: allowsNullParams
		if (x == QueryTerm.NULL
				&& fnct!=FNCT_OP.LIST_get
				&& fnct!=FNCT_OP.MAP_get
				&& fnct!=FNCT_OP.EQ
				&& fnct!=FNCT_OP.NE) {
			return QueryTerm.INVALID;
		}
		return x;
	}
	

	public boolean evaluateBool(Object currentInstance, Object globalInstance,
			VariableInstance[] vars, Object[] executionParams) {
		Object result = evaluate(currentInstance, globalInstance, vars, executionParams);
		//return false for invalid-result/null
		return result instanceof Boolean ? (Boolean)result : false; 
	}
	
	private Object evaluateFunction(Object li, Object gi, VariableInstance[] vars,
			Object[] executionParams) {
		if (li == QueryTerm.NULL || li == QueryTerm.INVALID) {
			//According to JDO spec 14.6.2, calls on 'null' result in 'false'
			switch (fnct) {
			case COLL_isEmpty: 
			case MAP_isEmpty: return li == QueryTerm.NULL;
			case COLL_contains: 
			case MAP_containsKey:
			case MAP_containsValue:
			case STR_startsWith:
			case STR_endsWith:
			case STR_matches:
			case STR_contains_NON_JDO: return false;
			default: return QueryTerm.INVALID;
			}
		}
		
		Object arg0 = param1 == null ? null : getValue(li, gi, param1, vars, executionParams);
		if (arg0 == QueryTerm.INVALID) {
			return QueryTerm.INVALID;
		}
		//Special optimization to avoid unnecessary evaluation of the right && operands:
		if (fnct == FNCT_OP.L_AND && !(Boolean)arg0) {
			return false;
		}
		if (fnct == FNCT_OP.L_OR && (Boolean)arg0) {
			return true;
		}
		Object arg1 = param2 == null ? null : getValue(li, gi, param2, vars, executionParams);
		if (arg1 == QueryTerm.INVALID) {
			return QueryTerm.INVALID;
		}

		switch (fnct) {
		case COLL_contains: return ((Collection<?>)li).contains(arg0);
		case COLL_isEmpty: return ((Collection<?>)li).isEmpty();
		case COLL_size: return ((Collection<?>)li).size();
		case LIST_get: 
			int posL =(int)arg0;
			int sizeL = ((List<?>)li).size();
			return (posL >= sizeL || posL < 0) ? QueryTerm.INVALID : ((List<?>)li).get(posL);
		case MAP_get: {
			Object ret =  ((Map<?, ?>)li).get(arg0);
			return ret == null ? QueryTerm.NULL : ret;
		}
		case MAP_containsKey: return ((Map<?,?>)li).containsKey(arg0) ;
		case MAP_containsValue: return ((Map<?,?>)li).containsValue(arg0);
		case MAP_isEmpty: return ((Map<?,?>)li).isEmpty();
		case MAP_size: return ((Map<?,?>)li).size();
		case STR_startsWith: return convertToString(li).startsWith(convertToString(arg0));
		case STR_endsWith: return convertToString(li).endsWith(convertToString(arg0));
		case STR_matches: return convertToString(li).matches(convertToString(arg0));
		case STR_contains_NON_JDO: return convertToString(li).contains(convertToString(arg0));
		case STR_indexOf1: return convertToString(li).indexOf(convertToString(arg0));
		case STR_indexOf2: return convertToString(li).indexOf(
				convertToString(arg0), 
				(Integer) arg1);
		case STR_substring1: return convertToString(li).substring((Integer) arg0);
		case STR_substring2: return convertToString(li).substring(
				(Integer) arg0, 
				(Integer) arg1);
		case STR_toLowerCase: return convertToString(li).toLowerCase();
		case STR_toUpperCase: return convertToString(li).toUpperCase();
		case STR_length: return convertToString(li).length();
		case STR_trim: return convertToString(li).trim();
		case Math_abs: {
			Class<?> oType = arg0.getClass();
			if (oType == Integer.class) {
				return Math.abs((Integer)arg0);
			} else if (oType == Long.class) {
				return Math.abs((Long)arg0);
			} else if (oType == Float.class) {
				return Math.abs((Float)arg0);
			} else if (oType == Double.class) {
				return Math.abs((Double)arg0);
			}
		}
		case Math_sqrt:
			double d = toDouble(arg0);
			return d >= 0 ? Math.sqrt(d) : QueryTerm.INVALID;
		case Math_sin:
			return Math.sin(toDouble(arg0));
		case Math_cos:
			return Math.cos(toDouble(arg0));
		case EQ_BOOL:
			return (boolean)arg0 == (boolean)arg1;
		case EQ:
			return equalsObject(arg0, arg1);
		case NE_BOOL:
			return (boolean)arg0 != (boolean)arg1;
		case NE:
			return !equalsObject(arg0, arg1);
		case L_AND:
			return (Boolean)arg0 && (Boolean)arg1;
		case L_OR:
			return (Boolean)arg0 || (Boolean)arg1;
		case L_NOT:
			return !(Boolean)arg0;
		case G:
			return compare(arg0, arg1) > 0;
		case GE:
			return compare(arg0, arg1) >= 0;
		case L:
			return compare(arg0, arg1) < 0;
		case LE:
			return compare(arg0, arg1) <= 0;
		case PLUS_STR:
			return convertToString(arg0) + convertToString(arg1);
		case PLUS_D:
			double d1 = TypeConverterTools.toDouble(arg0);
			double d2 = TypeConverterTools.toDouble(arg1);
			return d1 + d2;
		case PLUS_L:
			return TypeConverterTools.toLong(arg0) + TypeConverterTools.toLong(arg1);
		case MOD:
			return modulo(arg0, arg1);
//		case PLUS:
//			o1 = arg[0];
//			o2 = arg[1];
//			if (Double.class.isAssignableFrom(o1.getClass()) ||
//					Double.class.isAssignableFrom(o2.getClass()) ||
//					Float.class.isAssignableFrom(o1.getClass()) ||
//					Float.class.isAssignableFrom(o2.getClass())) {
//				double d1 = TypeConverterTools.toDouble(o1);
//				double d2 = TypeConverterTools.toDouble(o2);
//				return d1 + d2;
//			}
//			return TypeConverterTools.toLong(o1) + TypeConverterTools.toLong(o2);
//		case MINUS:
//			o1 = arg[0];
//			o2 = arg[1];
//			if (Double.class.isAssignableFrom(o1.getClass()) ||
//					Double.class.isAssignableFrom(o2.getClass()) ||
//					Float.class.isAssignableFrom(o1.getClass()) ||
//					Float.class.isAssignableFrom(o2.getClass())) {
//				double d1 = TypeConverterTools.toDouble(o1);
//				double d2 = TypeConverterTools.toDouble(o2);
//				return d1 - d2;
//			}
//			return TypeConverterTools.toLong(o1) - TypeConverterTools.toLong(o2);
//		case MUL:
//			o1 = arg[0];
//			o2 = arg[1];
//			if (Double.class.isAssignableFrom(o1.getClass()) ||
//					Double.class.isAssignableFrom(o2.getClass()) ||
//					Float.class.isAssignableFrom(o1.getClass()) ||
//					Float.class.isAssignableFrom(o2.getClass())) {
//				double d1 = TypeConverterTools.toDouble(o1);
//				double d2 = TypeConverterTools.toDouble(o2);
//				return d1 * d2;
//			}
//			return TypeConverterTools.toLong(o1) * TypeConverterTools.toLong(o2);
//		case DIV:
//			o1 = arg[0];
//			o2 = arg[1];
//			if (Double.class.isAssignableFrom(o1.getClass()) ||
//					Double.class.isAssignableFrom(o2.getClass()) ||
//					Float.class.isAssignableFrom(o1.getClass()) ||
//					Float.class.isAssignableFrom(o2.getClass())) {
//				double d1 = TypeConverterTools.toDouble(o1);
//				double d2 = TypeConverterTools.toDouble(o2);
//				return d1 / d2;
//			}
//			//This may lose some precision, but that's the same way as it happens in Java
//			return TypeConverterTools.toLong(o1) / TypeConverterTools.toLong(o2);
		case ENUM_ordinal:
			return ((Enum<?>)li).ordinal();
		case ENUM_toString:
			return ((Enum<?>)li).toString();
		default:
			throw new UnsupportedOperationException(fnct.name());
		}

//		switch (fnct) {
//		case COLL_contains: return ((Collection<?>)li).contains(getValue(li, gi, 0));
//		case COLL_isEmpty: return ((Collection<?>)li).isEmpty();
//		case COLL_size: return ((Collection<?>)li).size();
//		case LIST_get: 
//			int posL =(int)getValue(li, gi, 0);
//			int sizeL = ((List<?>)li).size();
//			return posL >= sizeL ? QueryTerm.INVALID : ((List<?>)li).get(posL);
//		case MAP_get: 
//			Object key = getValue(li, gi, 0);
//			if (key == QueryTerm.INVALID) {
//				return QueryTerm.INVALID;
//			}
//			return ((Map<?, ?>)li).get(key);
//		case MAP_containsKey: return ((Map<?,?>)li).containsKey(getValue(li, gi, 0)) ;
//		case MAP_containsValue: return ((Map<?,?>)li).containsValue(getValue(li, gi, 0));
//		case MAP_isEmpty: return ((Map<?,?>)li).isEmpty();
//		case MAP_size: return ((Map<?,?>)li).size();
//		case STR_startsWith: return ((String)li).startsWith((String) getValue(li, gi, 0));
//		case STR_endsWith: return ((String)li).endsWith((String) getValue(li, gi, 0));
//		case STR_matches: return ((String)li).matches((String) getValue(li, gi, 0));
//		case STR_contains_NON_JDO: return ((String)li).contains((String) getValue(li, gi, 0));
//		case STR_indexOf1: return ((String)li).indexOf((String) getValue(li, gi, 0));
//		case STR_indexOf2: return ((String)li).indexOf(
//				(String) getValue(li, gi, 0), 
//				(Integer) getValue(li, gi, 1));
//		case STR_substring1: return ((String)li).substring((Integer) getValue(li, gi, 0));
//		case STR_substring2: return ((String)li).substring(
//				(Integer) getValue(li, gi, 0), 
//				(Integer) getValue(li, gi, 1));
//		case STR_toLowerCase: return ((String)li).toLowerCase();
//		case STR_toUpperCase: return ((String)li).toUpperCase();
//		case Math_abs:
//			Object o = getValue(li, gi, 0);
//			if (o == QueryTerm.NULL || o == QueryTerm.INVALID) {
//				return QueryTerm.INVALID;
//			}
//			Class<?> oType = o.getClass();
//			if (oType == Integer.class) {
//				return Math.abs((Integer)o);
//			} else if (oType == Long.class) {
//				return Math.abs((Long)o);
//			} else if (oType == Float.class) {
//				return Math.abs((Float)o);
//			} else if (oType == Double.class) {
//				return Math.abs((Double)o);
//			}
//		case Math_sqrt:
//			o1 = getValue(li, gi, 0);
//			if (o1 == QueryTerm.NULL || o1 == QueryTerm.INVALID) {
//				return QueryTerm.INVALID;
//			}
//			double d = toDouble(o1);
//			return d >= 0 ? Math.sqrt(toDouble(o1)) : QueryTerm.INVALID;
//		case Math_sin:
//			o1 = getValue(li, gi, 0);
//			if (o1 == QueryTerm.NULL || o1 == QueryTerm.INVALID) {
//				return QueryTerm.INVALID;
//			}
//			return Math.sin(toDouble(o1));
//		case Math_cos:
//			o1 = getValue(li, gi, 0);
//			if (o1 == QueryTerm.NULL || o1 == QueryTerm.INVALID) {
//				return QueryTerm.INVALID;
//			}
//			return Math.cos(toDouble(o1));
//		case EQ_NUM:
//		case EQ_BOOL:
//		case EQ_OBJ:
//			o1 = getValue(li, gi, 0);
//			o2 = getValue(li, gi, 1);
//			if (o1 == QueryTerm.NULL || o1 == QueryTerm.INVALID ||
//					o2 == QueryTerm.NULL || o2 == QueryTerm.INVALID) {
//				return QueryTerm.INVALID;
//			}
//			if (o1 instanceof ZooPC) {
//				long oid1 = ((ZooPC)o1).jdoZooGetOid();
//				return (o2 instanceof ZooPC) ? ((ZooPC)o2).jdoZooGetOid() == oid1 : false;
//			}
//			return o1.equals(o2);
//		case PLUS:
//			o1 = (Number) getValue(li, gi, 0);
//			o2 = (Number) getValue(li, gi, 1);
//			if (o1 == QueryTerm.NULL || o1 == QueryTerm.INVALID ||
//					o2 == QueryTerm.NULL || o2 == QueryTerm.INVALID) {
//				return QueryTerm.INVALID;
//			}
//
//		default:
//			throw new UnsupportedOperationException(fnct.name());
//		}
	}

	private boolean equalsObject(Object o1, Object o2) {
		if (o1 == o2) {
			return true;
		}
		if (o1 == QueryTerm.NULL || o2 == QueryTerm.NULL) {
			return false;
		}
		if (o1 instanceof Number && o2 instanceof Number) {
			return compare(o1, o2) == 0;
		}
		if (o1 instanceof String ^ o2 instanceof String) {
			// only one side is a String, try to convert the other side to String, too.
			return compare(o1, o2) == 0;
		}
		if (o1 instanceof ZooPC && o2 instanceof ZooPC) {
			long oid1 = ((ZooPC)o1).jdoZooGetOid();
			long oid2 = ((ZooPC)o2).jdoZooGetOid();
			return oid1 == oid2;
		}
		return Objects.equals(o1, o2);
	}

	private int compare(Object n1, Object n2) {
		return compare(comparisonType, n1, n2);
	}

	private int compare(COMPARISON_TYPE ct, Object n1, Object n2) {
		switch (ct) {
		case BIG_DECIMAL:
			return ((BigDecimal)n1).compareTo((BigDecimal)n2);
		case BIG_INT:
			return ((BigInteger)n1).compareTo((BigInteger)n2);
		case LONG:
			return Long.compare(toLong(n1), toLong(n2));
		case INT:
			return Integer.compare(toInt(n1), toInt(n2));
		case DOUBLE:
			return Double.compare(toDouble(n1), toDouble(n2));
		case FLOAT:
			return Float.compare(toFloat(n1), toFloat(n2));
		case STRING:
			return convertToString(n1).compareTo(convertToString(n2));
		case CHAR:
			return Character.compare((Character)n1, (Character) n2);
		case PC:
			
		case SCO: //TODO comparable SCO???
			
		default: 
			//attempt late binding
			if (n1 instanceof Number && n2 instanceof Number) {
				COMPARISON_TYPE ct2 = TypeConverterTools.fromTypes(n1.getClass(), n2.getClass());
				return compare(ct2, n1, n2);
			}
			throw DBLogger.newUser("Comparison not supported for: " + comparisonType);
		}
	}
	
	private Object modulo(Object n1, Object n2) {
		if (n1 == null || n2 == null) {
			throw DBLogger.newUser("Cannot calculate % with null operand.");
		}
		switch (comparisonType) {
		case BIG_INT:
			return ((BigInteger)n1).mod((BigInteger)n2);
		case LONG:
			return toLong(n1) % toLong(n2);
		case INT:
			return toInt(n1) % toInt(n2);
		case DOUBLE:
			return toDouble(n1) % toDouble(n2);
		case FLOAT:
			return toFloat(n1) % toFloat(n2);
		default: 
			throw DBLogger.newUser("Comparison not supported for: " + comparisonType);
		}
	}
	
	@Override
	public String toString() {
//		return fnct.name() + "[" + (field != null ? field.getName() : null) + 
//				"](" + Arrays.toString(params) + ")";
//		return (field != null ? field.getName() : fnct.name()) 
//				+ "(" + Arrays.toString(params) + ")";
		String str = "";
		switch (fnct) {
		case THIS: return "THIS";
		case FIELD: 
			str += field.getName(); break;
		case CONSTANT:
			str += constant; break;
		case VARIABLE:
			str += "VAR_" + ((QueryVariable)constant).getName(); break;
		default: str += fnct.name();
		}
		str += "[";
		str += isConstant ? "c" : "";  
		str += isDependentOnParameter ? "p" : "";  
		str += "]";
		str += "(";
		str += param0 != null ? param0 : "";  
		str += "," + (param1 != null ? param1 : "");  
		str += "," + (param2 != null ? param2 : "");
		str += ")";
		return str;
		//return str +"(" + param0 + "," + param1 + "," + param2 + ")";
	}

	public FNCT_OP op() {
		return fnct;
	}

	public Object getConstant(Object[] params) {
		if (fnct == FNCT_OP.PARAM) {
			ParameterDeclaration p = (ParameterDeclaration) constant;
			return p.getValue(params);
		}
		if (isConstant) {
			return constant;
		}
		throw new IllegalStateException();
	}

	/**
	 * WARNING: This will return ParameterDeclaration objects! 
	 * @return the data object
	 */
	public Object getConstantUnsafe() {
		if (isConstant || fnct == FNCT_OP.PARAM) {
			return constant;
		}
		throw new IllegalStateException();
	}

	public ZooFieldDef getFieldDef() {
		return zField;
	}

	public boolean isPC() {
		return returnTypeDef != null;
	}

	public ZooClassDef getReturnTypeClassDef() {
		return returnTypeDef;
	}
	
	public QueryFunction getParam0() {
		return param0;
	}

	public QueryFunction getParam1() {
		return param1;
	}

	/**
	 * @param alternativeProposals List of index proposals
	 * @param params Query execution parameters
	 * @return 'true' if an index was found, otherwise 'false'.
	 */
	public boolean determineIndexToUseSubForQueryFunctions(
			IndexProposalSet[] alternativeProposals, Object[] params) {
		QueryFunction arg1, arg2;

//		TODO: add to alternatives when returning true
//		TODO: add to alternatives when returning true
//		TODO: add to alternatives when returning true
//		TODO: add to alternatives when returning true
//		TODO: add to alternatives when returning true
//		TODO: add to alternatives when returning true
//		TODO: add to alternatives when returning true
//		TODO: add to alternatives when returning true
//		TODO: add to alternatives when returning true
		
		switch (fnct) {
		case L_AND: {
			return analyzeAND(this, alternativeProposals, params);
		}
		case L_OR:
			return analyzeOR(this, alternativeProposals, params); 
		case COLL_contains:
		case MAP_containsKey:
		case MAP_containsValue:
			if (param1.op() != FNCT_OP.VARIABLE) {
				return false;
			}
			//Signal that we can use a constrained query
			if (alternativeProposals[param1.fieldId] == null) {
				alternativeProposals[param1.fieldId] = new IndexProposalSet();
			}
			alternativeProposals[param1.fieldId].addCollectionConstraint(this);
			return true;
		case STR_matches:
		case STR_startsWith:
			//STR_matches works only if it is effectively '==' or 'startsWith'. 
			//Right side has to be constant (startsWith). If it is not, we could
			//at most index the first letter...
			if (!param0.isFixed() && param1.isFixed()) {
				arg1 = param0;
				arg2 = param1;
				break;
			}
			return false;
		case EQ_BOOL:
		case EQ:
			analyzeEQ(this, alternativeProposals);
			//No break or return here!!!!
		case NE_BOOL:
		case NE:
		case LE:
		case L:
		case GE:
		case G:
			if (param1.isFixed() ^ param2.isFixed()) {
				//Yay!
				if (param2.isFixed()) {
					arg1 = param1;
					arg2 = param2;
				} else {
					arg1 = param2;
					arg2 = param1;
				}
				break;
			}
		default: 
			return false;
		}
		
		switch (arg1.op()) {
		case REF:
		case FIELD:
			//we can use index only when operating on a local field 
			break;
			default: 
				return false;
		}
		
		ZooFieldDef zField = arg1.zField; 
		if (zField == null || !zField.isIndexed()) {
			//ignore fields that are not index
			return false;
		}

		//TODO
		//TODO
		//TODO
		//TODO allow variable!=THIS
		if (arg1.getParam0().op() != FNCT_OP.THIS) {
			//TODO we don't support path queries yet, i.e. the string field must belong to
			//the currently evaluated main-object, not to a referenced object.
			return false;
		}
		//TODO
		//TODO
		//TODO
		//TODO choose variable!
		if (alternativeProposals[0] == null) {
			alternativeProposals[0] = new IndexProposalSet();
		}
		IndexProposalSet indexProposal = alternativeProposals[0];
		//even if we don't narrow the values, min/max allow ordered traversal
		indexProposal.initMinMax(zField);
		
		
//		TODO
//		- use indexProposalMap.addMin/max
//		- assign every affected queryNode a QueryAdvice (or IndexProposalSet?) so
//		  it knows when to init an iterator. 'Init' means att sub-iterator or values
//		  to variable-iterator
		
		switch (fnct) {
		case STR_matches: {
			if (arg1.op() == FNCT_OP.PARAM) {
				//TODO
				//TODO
				//TODO
				throw new UnsupportedOperationException();
			}
			String str = (String) arg2.getConstant(params);
			boolean isParam = arg2.isDependentOnParameter();
			for (int i = 0; i < str.length(); i++) {
				char c = str.charAt(i);
				if (QueryOptimizer.REGEX_CHARS[c]) {
					//if we have a regex that does not simply result in full match we
					//simply use the leading part for a startsWith() query.
					if (i == 0) {
						QueryImpl.LOGGER.info("Ignoring index on String query because of regex characters.");
						return false;
					}
					str = str.substring(0, i);
					setKeysForStringStartsWith(str, zField, indexProposal, isParam);
					return true;
				}
			}
			long key = BitTools.toSortableLong(str);
			indexProposal.addMin(zField, key, isParam);
			indexProposal.addMax(zField, key, isParam);
			break;
		}
		case STR_startsWith:
			if (arg1.op() == FNCT_OP.PARAM) {
				//TODO
				//TODO
				//TODO
				throw new UnsupportedOperationException();
			}
			String str = (String) arg2.getConstant(params);
			boolean isParam = arg2.isDependentOnParameter();
			setKeysForStringStartsWith(str, zField, indexProposal, isParam);
			break;
		default: //nothing
		}

		Object termVal = arg2.getConstant(params);//term.getValue(null);
		//TODO if(term.isRef())?!?!?!
		//TODO implement term.isIndexable() ?!?!?
		//TODO swap left/right side of query term such that indexed field is always on the left
		//     and the constant is on the right.
		boolean isParam = arg2.isDependentOnParameter();
		long value;
		ZooFieldDef f = zField; //TODO simplify
		switch (f.getJdoType()) {
		case PRIMITIVE:
			switch (f.getPrimitiveType()) {
			case BOOLEAN:   
				//pointless..., well pretty much, unless someone uses this to distinguish
				//very few 'true' from many 'false' or vice versa.
				return false;
			case DOUBLE: value = BitTools.toSortableLong(TypeConverterTools.toDouble(termVal)); 
			break;
			case FLOAT: value = BitTools.toSortableLong(TypeConverterTools.toFloat(termVal)); 
			break;
			case CHAR:
				if (termVal instanceof String) {
					throw DBLogger.newUser("Cannot compare String to Character");
				}
				value = (long)((Character)termVal).charValue(); break;
			case BYTE:
			case INT:
			case LONG:
			case SHORT:	value = ((Number)termVal).longValue(); break;
			default: 				
				throw new IllegalArgumentException("Type: " + f.getPrimitiveType());
			}
			break;
		case STRING:
			termVal = termVal instanceof Character ? String.valueOf((Character) termVal) : termVal;
			value = BitTools.toSortableLong(
					termVal == QueryTerm.NULL ? null : (String)termVal); 
			break;
		case REFERENCE:
			value = (termVal == QueryTerm.NULL ? 
					BitTools.NULL : ((ZooPC)termVal).jdoZooGetOid());
			break;
		case DATE:
			value = (termVal == QueryTerm.NULL ? 0 : ((Date)termVal).getTime()); 
			break;
		default:
			throw new IllegalArgumentException("Type: " + f.getJdoType());
		}

		switch (op()) {
		case EQ_BOOL:
		case EQ: 
			//TODO check range and exit if EQ does not fit in remaining range
			indexProposal.addMin(f, value, isParam);
			indexProposal.addMax(f, value, isParam);
			break;
		case L:
			//this also work with floats!
			indexProposal.addMax(f, value - 1, isParam);
			break;
		case LE: 				
			indexProposal.addMax(f, value, isParam);
			break;
		case G: 
			//this also work with floats!
			indexProposal.addMin(f, value + 1, isParam);
			break;
		case GE:
			indexProposal.addMin(f, value, isParam);
			break;
		case NE_BOOL:
		case NE:
		case STR_matches:
		case STR_contains_NON_JDO:
		case STR_endsWith:
			//ignore
			break;
		case STR_startsWith:
			setKeysForStringStartsWith((String) termVal, f, indexProposal, isParam);
			break;
		default: 
			throw new IllegalArgumentException("Name: " + op());
		}
		
		return true;
	}
	
	boolean isDependentOnParameter() {
		return isDependentOnParameter;
	}

	private static void analyzeEQ(QueryFunction f, IndexProposalSet[] alternativeProposals) {
		if (f.param1.op() == FNCT_OP.VARIABLE) {
			if (alternativeProposals[f.param1.fieldId] == null) {
				alternativeProposals[f.param1.fieldId] = new IndexProposalSet();
			}
			alternativeProposals[f.param1.fieldId].addIdentityConstraint(
					f.param2, f.param2.isDependentOnParameter);
		}
		if (f.param2.op() == FNCT_OP.VARIABLE) {
			if (alternativeProposals[f.param2.fieldId] == null) {
				alternativeProposals[f.param2.fieldId] = new IndexProposalSet();
			}
			alternativeProposals[f.param2.fieldId].addIdentityConstraint(
					f.param1, f.param1.isDependentOnParameter);
		}
	}

	private void setKeysForStringStartsWith(String prefix, ZooFieldDef f,
			IndexProposalSet indexProposal, boolean isParam) {
		long keyMin = BitTools.toSortableLongPrefixMinHash(prefix);
		long keyMax = BitTools.toSortableLongPrefixMaxHash(prefix);
		indexProposal.addMin(f, keyMin, isParam);
		indexProposal.addMax(f, keyMax, isParam);
	}


	private static boolean analyzeAND(QueryFunction f, IndexProposalSet[] alternativeProposals,
			Object[] executionParams) {
		//ensure that we always evaluate both branches!
		boolean b1 = f.param1.determineIndexToUseSubForQueryFunctions(alternativeProposals, executionParams);
		boolean b2 = f.param2.determineIndexToUseSubForQueryFunctions(alternativeProposals, executionParams);
		return b1 && b2;
	}

	
	private static boolean analyzeOR(QueryFunction f, IndexProposalSet[] alternativeProposals,
			Object[] executionParams) {
		//We should return a QueryAdvice only if both branches are indexable...
		IndexProposalSet[] proposalsL = new IndexProposalSet[alternativeProposals.length];
		boolean b1 = f.param1.determineIndexToUseSubForQueryFunctions(proposalsL, executionParams);
		
		IndexProposalSet[] proposalsR = new IndexProposalSet[alternativeProposals.length];
		boolean b2 = f.param2.determineIndexToUseSubForQueryFunctions(proposalsR, executionParams);

		if (!(b1 && b2)) {
			//Let's try later
			return false;
		}
		
		for (int varId = 0; varId < alternativeProposals.length; varId++) {
			IndexProposalSet propL = proposalsL[varId];
			IndexProposalSet propR = proposalsR[varId];
			//Only create an index if both sides can provide some sort of constraint. Otherwise
			//we are better off with an Extent. Or, if the variable is not used in this branch,
			//we do not need an index anyway (but we would need one for the other branch....?)
			if (propL != null && propR != null) {
				List<IndexProposalSet> cumulativeProposal = new ArrayList<>();
				cumulativeProposal.add(propL);
				cumulativeProposal.add(propR);
				if (alternativeProposals[varId] == null) {
					alternativeProposals[varId] = new IndexProposalSet();
				}
				alternativeProposals[varId].addCumulative(cumulativeProposal);
			}
		}
		return true;
	}

	
	
	
	boolean isConstrained(boolean[] constrainedVars) {
		//The idea is to find out which branches in a query term are constrained.
		//This allows us to reorder the query tree such that Variables are
		//always assigned (occur in a constrained branch) before we need their value
		//in an unconstrained branch.
		//Approach: At every <=,>=,>,< and != , we look at both branches.
		//If one branch is unconstrained, we swap the side so that it is on the right side.
		//This ensures that unconstrained branches move to the end of the evaluation.
		//If there are only unconstrained branches, then the variable itself is unconstrained
		//and the query is invalid.
		
		//shortcut -> this is set to 'true' once we know that this branch is constrained
		if (isConstrained) {
			return true;
		}
		
		if (isFixed()) {
			//constants, parameters and THIS -> these are obviously constrained
			isConstrained = true;
			return true;
		}
		switch (fnct) {
		case EQ:
		case EQ_BOOL:
			//If only one is constrained, we can use it to constrain the other (assignment)
//			isConstrained = param2.isConstrained(constrainedVars) || param1.isConstrained(constrainedVars);
			return param2.isConstrained(constrainedVars) && param1.isConstrained(constrainedVars);
		case COLL_contains:
		case MAP_containsKey:
		case MAP_containsValue:
			//TODO if param0 (the base object) is unconstrained, we need
			//at least a reverse index...
//			isConstrained = param1.isConstrained(constrainedVars) || param0.isConstrained(constrainedVars);
			return param1.isConstrained(constrainedVars) && param0.isConstrained(constrainedVars);
		case VARIABLE:
			return constrainedVars[fieldId];
		default:
			boolean b0 = param0 == null || param0.isConstrained(constrainedVars);
			boolean b1 = param1 == null || param1.isConstrained(constrainedVars);
			boolean b2 = param2 == null || param2.isConstrained(constrainedVars);
			return b0 && b1 && b2;
		}
	}
	

	static final double COST_ASSIGN_VAR = 100001;
	static final double COST_FIELD_ASSIGN = 100002;

	enum Constraint {
		UNUSED(0),
		
//		/** Constrained. */
//		YES(-1),
//		/** Unconstrained. */
//		NO(1),
//		/** Depends on other variable. */
//		DEPENDS(0),
		UNCONSTRAINED(Double.POSITIVE_INFINITY),
		OBJ_UNCONSTRAINED(QueryAdvice.COST_EXTENT),
		PRIM_UNCONSTRAINED(Double.POSITIVE_INFINITY),

		/** 
		 * Primitive variable assignment:  int v = 3;  int v = :param .
		 * Object variable: MyClass x = this.refX . 
		 */
		ASSIGN(1),
		USE_CONSTRAINED_VAR(1),
		
		/** Primitive Variable comparison or assignment: int v1 = int v2 */
		PRIM_ASSIGN_VAR(COST_ASSIGN_VAR),
		/** Primitive variable: this.list.contains(int X) */
		PRIM_CONTAINS(QueryAdvice.COST_CONTAINS),
		/** Object variable: this.list.contains(MyClass X) */
		OBJ_CONTAINS(QueryAdvice.COST_CONTAINS),
		/** Object field assign (requires index): this.field = (MyClass)x.field */ 
		OBJ_FIELD_ASSIGN(COST_FIELD_ASSIGN)
		;
		
		final double cost;
		Constraint(double cost) {
			this.cost = cost;
		}
		public static boolean isConstrained(double c) {
			return c > UNUSED.cost && c < UNCONSTRAINED.cost;
		}
		public static boolean isUnused(double d) {
			return d == UNUSED.cost;
		}
		
	}

	
	/**
	 * Reorder query such that Variables are assigned before they are used.
	 * 
	 * Concept:
	 * 1) Traverse query tree from left to right.
	 * 2) For every '==' (and 'contains()'), check whether the left and right
	 *    branch are 'ok' (fully or mostly constrained): 
	 *    If all comparisons are between assigned Variables or constants, mark branch as 'ok'.
	 *    If branch contains assignments of variables (var v = 3), also mark
	 *    variable as 'ok'.
	 *    Otherwise, mark as 'unconstrained'
	 * 3) If both side are 'ok', mark comparison as 'ok' and move further up.
	 *    Continue with 1)/2)
	 * 4) If right branch is 'ok' and left is 'unconstrained', swap branches.
	 *    Then try the unconstrained side again (it may now be constrained,
	 *    due to a possible Variable assignment in the (new) left branch.
	 * 5) If both sides are unconstrained or if right is unconstrained after 4),
	 *    mark '==' as unconstrained and move back up, continue with 1)/2).
	 *    
	 * This approach should always find an ordering that assigns all variables
	 * before they are used.
	 * There are some possible optimizations:
	 * 1) Not all assignments are equal:
	 *    a) Direct assignment is best: var x = y
	 *    b) 'contains()' is still good but a little more expensive
	 *    c) Indirect assignment is only good if there is an index on 'field', 
	 *       but then it is still O(log n):   var x.field = y
	 *       This can also work with comparison greater, greater-eq, less, less-eq!  
	 *    d) Unconstrained (persistent) objects are bad, but manageable, they use
	 *       an extent.
	 *    e) Unconstrained 'countable' integer/char is manageable, depending on the 
	 *       range. This may be better than d).
	 *    f) Unconstrained 'uncountable' float/date is not manageable (except microseconds?).
	 *
	 *    For now we assume that f) fails.
	 *    Generally, the above means that some 'ok' are better than others.
	 *    We could estimate the cost (cardinality) and reorder the branches
	 *    again if we find multiple ways to constrain a variable. However this
	 *    is complicated because it may invalidate constraining of other variables...
	 *    
	 * 2) The concept describe that sub-trees are parsed multiple time until 
	 *    their variables are constrained. This could be optimized by
	 *    maintaining for each sub-branch a list of its unconstrained variables.
	 *    The branch is only parsed again (or simply marked as 'ok') if the
	 *    associated constrained variables have changed their status to 'ok'.
	 *    We could even attach a list of unconstrained branches to each variable.
	 *    
	 * Question:
	 * Should we merge the constraint-resolving with index detection? 
	 * Otherwise, should we do it before or after index detection?
	 * To consider:
	 * #1 (Obviously )Not all orderings are valid.
	 * #2 We may want to reorder the tree in order to take advantage of a given index.
	 *    For example: "v1.field1 = 23 && v1.field2 == v2.field2 && v2.field1 = 25"
	 *    Both variables can use an index, but most efficiently, we would use only one index
	 *    and then use the middle term to assign the second variable. But which index should we use?
	 *    This depends on the selectivity.
	 * #3 Is it possible that the index selection recommends an ordering that is not 'possible'
	 *    (fully constrained) ?
	 *   
	 * - BEFORE: Fast way to create a valid ordering. Index detection is simplified, it does
	 *           not consider different orderings(?). Avoid
	 * - MERGED: Complex to implements, probably expensive because of all the rolling back and 
	 *           temporary variables that need to be merged into the parent.
	 * - MIXED:  Test for validity (and reorder) sub-branches separately from detecting indexes.
	 *           This avoid complex rollback of 
	 * - AFTER:  Choose best indexing, then find valid ordering. Can this go wrong? See #3. 
	 * 
	 * Variable-Ordering and index detection are similar, but not the same.
	 * Variable ordering ensures that there exist a valid constraint.
	 * 
	 * Index detection is independent of ordering EXCEPT when two variables are compared...
	 * Solution:
	 * - Detect indexes
	 * - Then reorder for validity (and optimality?)
	 * Advantage: Reordering can combine validity with cost-consideration! 
	 * 
	 * 
	 * TODO
	 * Ordering based on 'priority'?
	 * 
	 * 
	 * 
	 * Total cost calculation of query: 
	 * Simply said, it is the product of all Variable-costs, ie. the product-sum of the 
	 * constrained vars[].
	 * However, the array is determined hierarchically: See external Query Design document.
	 * 
	 * 
	 * @param constrainedVars Variable constraints that are known from 'above'
	 * @param vars Variables and index propositions
	 * @param canBeAssigned Whether this current term is on the right side of '==', i.e. if it 
	 * can be assigned a value from the left operand.
	 * 
	 */
	double resolveMissingConstraintsByReordering(double[] constrainedVars, VariableInstance[] vars, 
			boolean canBeAssigned) {
		qa = null;
		qaType = null;
		double[] backup = Arrays.copyOf(constrainedVars, constrainedVars.length);
		
		switch (fnct) {
		case L_AND: {
			double b1 = param1.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			double b2 = param2.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			if (!Constraint.isConstrained(b1)) {
				if (!Constraint.isConstrained(b2)) {
					//both are unconstrained, this is bad
					return Constraint.PRIM_UNCONSTRAINED.cost;
				}
				//Okay, swap and try again.
				//First, restore vars from backup, param1 is unconstrained, but it
				//may still have initialized a variable that is used by param2.
				System.arraycopy(backup, 0, constrainedVars, 0, backup.length);
				//try again, in reverse order
				b1 = param2.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
				b2 = param1.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
				//TODO clean up
				//-> We swap in any case! Swapping hurts very little, but the QA's are now 
				//initialized for the swapped version
//				if (Constraint.isConstrained(b1) && Constraint.isConstrained(b2)) {
//					//it worked -> swap
					QueryFunction f = param1;
					param1 = param2;
					param2 = f;
//				} else {
//					//restore
//					System.arraycopy(backup, 0, constrainedVars, 0, backup.length);
//				}
			}
			return b1 * b2;
		}
		case L_OR: {
			//Nothing to do here, reordering does not help with Variable constraint resolution.
			//TODO However, reordering OR may only help to improve performance by moving
			//     terms with better selectivity to the left.
			double b1 = param1.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			double b2 = param2.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			return b1 + b2;
		}
		case EQ_BOOL:
		case EQ: {
			double b1 = param1.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			double b2 = param2.resolveMissingConstraintsByReordering(constrainedVars, vars, true);
			if (!Constraint.isConstrained(b1)) {
				if (!Constraint.isConstrained(b2)) {
					//both are unconstrained, this is bad
					return Constraint.PRIM_UNCONSTRAINED.cost;
				}
				//Okay, swap and try again.
				//First, restore vars from backup, param1 is unconstrained, but it
				//may still have initialized a variable that is used by param2.
				System.arraycopy(backup, 0, constrainedVars, 0, backup.length);
				//swap
				QueryFunction f = param1;
				param1 = param2;
				param2 = f;
				b1 = param1.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
				b2 = param2.resolveMissingConstraintsByReordering(constrainedVars, vars, true);
			}
			if (Constraint.isConstrained(b1) && !Constraint.isConstrained(b2)) {
				b2 = offerConstraint(param2, constrainedVars);
			}
		//TODO multiply???
			return b1 * b2;
		}
		case NE_BOOL:
		case NE:
		case LE:
		case L:
		case GE:
		case G: {
			//Reordering comparators (other than EQ) does not make much sense:
			//- LE,L,GE,G compare only Date, Numeric and String values. These can't 
			//  be terms that constrain a variable.
			//  They may depend on a variable, including sub-queries, but they cannot define it.
			double b1 = param1.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			double b2 = param2.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			return b1 * b2;
		}
		case VARIABLE:
			if (Constraint.isUnused(constrainedVars[fieldId])) {
				//TODO lookup index, extent...
				boolean isPrimitive = vars[fieldId].var.getTypeDef() == null;
				double r;
				if (canBeAssigned) {
					//Unassigned and can be assigned?
					//We always prefer assign over any other way, such as JOIN. We need to think
					//about whether this is always good....
					r = Constraint.ASSIGN.cost;
					this.qaType = QueryAdvice.Type.IDENTITY;
				} else if (isPrimitive) {
					//indicate failure
					r = Constraint.PRIM_UNCONSTRAINED.cost;
				} else {
					List<QueryAdvice> advices = vars[fieldId].getAdvices();
					if (advices.isEmpty()) {
						//extent
						r = Constraint.OBJ_UNCONSTRAINED.cost;
					} else {
						//TODO get all
						QueryAdvice any = vars[fieldId].getAdvices().get(0);
						r = any.getCost();
						this.qa = any;
						this.qaType = any.getType();
					}
				}
				constrainedVars[fieldId] = r;
				//return actual cost
				return r;
			}

			if (Constraint.isConstrained(constrainedVars[fieldId])) {
				return Constraint.USE_CONSTRAINED_VAR.cost;
			}
			return vars[fieldId].var.getTypeDef() != null ? 
					Constraint.OBJ_UNCONSTRAINED.cost : Constraint.PRIM_UNCONSTRAINED.cost;
		case COLL_contains:
		case MAP_containsKey:
		case MAP_containsValue: {
			double b0 = param0.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			double b1 = param1.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
			if (!Constraint.isConstrained(b0)) {
				//TODO if param0 (the base object) is unconstrained, we could use an reverse index, 
				//     once implemented...
				return b0;
			}
			if (Constraint.isConstrained(b1)) {
				//Cross product...
				return b0 * b1;
			}
			//Okay, we use 'contains()' to constrain the variable
			double cost = b0 * Constraint.OBJ_CONTAINS.cost;
			constrainedVars[fieldId] = cost;
			return cost;
		}
		case THIS: 
			//TODO remove
			if (fieldId != 0) {
				throw new IllegalStateException("fieldId=" + fieldId);
			}
			
			double r;
			if (Constraint.isUnused(constrainedVars[fieldId])) {
				List<QueryAdvice> advices = vars[fieldId].getAdvices();
				if (advices.isEmpty()) {
					//extent
					r = Constraint.OBJ_UNCONSTRAINED.cost;
					this.qaType = QueryAdvice.Type.EXTENT;
				} else {
					//TODO get all
					QueryAdvice any = vars[fieldId].getAdvices().get(0);
					r = any.getCost();
					this.qa = any;
					this.qaType = any.getType();
				}
				constrainedVars[fieldId] = r;
				return r;
			}
			return constrainedVars[fieldId];
		case CONSTANT:
		case PARAM:
			return 1;
		default:
			break;
		}

		//Why multiply? Why not?
		double result = 1;
		if (param0 != null) {
			result *= param0.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
		}
		if (param1 != null) {
			result *= param1.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
		}
		if (param2 != null) {
			result *= param2.resolveMissingConstraintsByReordering(constrainedVars, vars, false);
		}
		if (param0 == null && param1 == null && param2 == null) {
			throw new IllegalStateException("op=" + op());
		}
		return result;
	}
	
	@Deprecated
	boolean resolveMissingConstraintsByReorderingOld(boolean[] constrainedVars) {
		qa = null;
		if (isConstrained) {
			return true;
		}

		boolean[] backup = Arrays.copyOf(constrainedVars, constrainedVars.length);
		
		//Check children
//		boolean result = true;
//		if (param0 != null) {
//			result &= param0.resolveMissingConstraintsByReordering(constrainedVars);
//		}
//		if (param1 != null) {
//			result &= param1.resolveMissingConstraintsByReordering(constrainedVars);
//		}
//		if (param2 != null) {
//			result &= param2.resolveMissingConstraintsByReordering(constrainedVars);
//		}
		
		switch (fnct) {
		case L_AND: {
			boolean b1 = param1.resolveMissingConstraintsByReorderingOld(constrainedVars);
			boolean b2 = param2.resolveMissingConstraintsByReorderingOld(constrainedVars);
			if (!b1) {
				if (!b2) {
					//both are unconstrained, this is bad
					return false;
				}
				//Okay, swap and try again.
				//First, restore vars from backup, param1 is unconstrained, but it
				//may still have initialized a variable that is used by param2.
				System.arraycopy(backup, 0, constrainedVars, 0, backup.length);
				//try again, in reverse order
				b1 = param2.resolveMissingConstraintsByReorderingOld(constrainedVars);
				b2 = param1.resolveMissingConstraintsByReorderingOld(constrainedVars);
				if (b1 && b2) {
					//it worked -> swap
					QueryFunction f = param1;
					param1 = param2;
					param2 = f;
				} else {
					//restore
					System.arraycopy(backup, 0, constrainedVars, 0, backup.length);
				}
			}
			return b1 && b2;
		}
		case L_OR:
			//Nothing to do here, reordering does not help with Variable constraint resolution.
			//TODO However, reordering OR may only help to improve performance by moving
			//     terms with better selectivity to the left.
			break;
		case EQ_BOOL:
		case EQ: {
			boolean b1 = param1.resolveMissingConstraintsByReorderingOld(constrainedVars);
			boolean b2 = param2.resolveMissingConstraintsByReorderingOld(constrainedVars);
			if (!b1) {
				if (!b2) {
					//both are unconstrained, this is bad
					return false;
				}
				//Okay, swap and try again.
				//First, restore vars from backup, param1 is unconstrained, but it
				//may still have initialized a variable that is used by param2.
				System.arraycopy(backup, 0, constrainedVars, 0, backup.length);
				//swap
				QueryFunction f = param1;
				param1 = param2;
				param2 = f;
				b1 = param1.resolveMissingConstraintsByReorderingOld(constrainedVars);
				b2 = param2.resolveMissingConstraintsByReorderingOld(constrainedVars);
			}
			if (b1 && !b2) {
				b2 = offerConstraint(param2, constrainedVars);
			}
			return b1 && b2;
		}
		case NE_BOOL:
		case NE:
		case LE:
		case L:
		case GE:
		case G:
			//Reordering comparators (other than EQ) does not make much sense:
			//- LE,L,GE,G compare only Date, Numeric and String values. These can't 
			//  be terms that constrain a variable.
			//  They may depend on a variable, including sub-queries, but they cannot define it.
			break;
		case VARIABLE:
			return constrainedVars[fieldId];
		case COLL_contains:
		case MAP_containsKey:
		case MAP_containsValue: {
//			//TODO if param0 (the base object) is unconstrained, we need
//			//at least a reverse index...
//			isConstrained = param1.isConstrained(constrainedVars) || param0.isConstrained(constrainedVars);
//			break;
			boolean b1 = param1.isConstrained(constrainedVars);
			boolean b0 = param0.isConstrained(constrainedVars);
			return b1 && b0;
		}
		default:
			break;
		}

		boolean result = true;
		if (param0 != null) {
			result &= param0.resolveMissingConstraintsByReorderingOld(constrainedVars);
		}
		if (param1 != null) {
			result &= param1.resolveMissingConstraintsByReorderingOld(constrainedVars);
		}
		if (param2 != null) {
			result &= param2.resolveMissingConstraintsByReorderingOld(constrainedVars);
		}
		return result;
	}
	
	/**
	 * This is called on one side of '==' when the other side is 
	 * 'fixed' (constant, param or constrained variable).  
	 * @param f The QF to check.
	 */
	private static boolean offerConstraint(QueryFunction f, boolean[] constrainedVars) {
		switch (f.fnct) {
		case VARIABLE:
			constrainedVars[f.fieldId] = true;
			return true;
		case FIELD:
			if (f.getFieldDef().isIndexed()) {
				return offerConstraint(f.param0, constrainedVars);
			}
			return false;
		default:
			break;
		}
		return false;
	}
	
	/**
	 * This is called on one side of '==' when the other side is 
	 * 'fixed' (constant, param or constrained variable).  
	 * @param f The QF to check.
	 */
	private static double offerConstraint(QueryFunction f, double[] constrainedVars) {
		switch (f.fnct) {
		case VARIABLE:
			double r;
			if (Constraint.isUnused(constrainedVars[f.fieldId])) {
				r = Constraint.ASSIGN.cost;
			} else {
				r = Constraint.USE_CONSTRAINED_VAR.cost; 
			}
			constrainedVars[f.fieldId] = r;
			return r;
		case FIELD:
			if (f.getFieldDef().isIndexed()) {
				return offerConstraint(f.param0, constrainedVars);
			}
			return f.getFieldDef().isPersistentType() 
					? Constraint.OBJ_UNCONSTRAINED.cost 
							: Constraint.PRIM_UNCONSTRAINED.cost;
		default:
			throw new UnsupportedOperationException(f.fnct.name());
		}
	}
}
