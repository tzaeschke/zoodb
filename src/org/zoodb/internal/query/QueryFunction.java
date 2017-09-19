/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.internal.query;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryParser.FNCT_OP;

import static org.zoodb.internal.query.TypeConverterTools.*;

/**
 * Query functions.
 * 
 * @author ztilmann
 *
 */
public class QueryFunction {

	private final Class<?> returnType;
	private final ZooClassDef returnTypeDef;
	private final FNCT_OP fnct;
	private final int fieldId;
	private final Field field;
	private final ZooFieldDef zField;
	private final QueryFunction[] params;
	private final Object constant;
	private boolean isConstant; //indicate whether evaluation is constant
	
	private QueryFunction(FNCT_OP fnct, ZooFieldDef zField, 
			Object constant, Class<?> returnType, ZooClassDef returnTypeDef, 
			QueryFunction ... params) {
		this.returnType = returnType;
		this.returnTypeDef = returnTypeDef;
		this.fnct = fnct;
		this.zField = zField;
		if (zField != null) {
			fieldId = zField.getFieldPos();
			field = zField.getJavaField();
		} else {
			fieldId = -1;
			field = null;
		}
		this.constant = constant;
		this.params = params;
		this.isConstant = fnct == FNCT_OP.CONSTANT;
		if (params.length > 0) {
			this.isConstant = true;
			for (QueryFunction f: params) {
				if (!f.isConstant()) {
					this.isConstant = false;
					break;
				}
			}
		}
	}
	
	/**
	 * 
	 * @return 'true' if this sub-tree yields a constant result, independent of parameters etc
	 */
	public boolean isConstant() {
		return isConstant;
	}

	public Class<?> getReturnType() {
		return returnType;
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
	
	public static QueryFunction createJava(FNCT_OP op, QueryFunction ... params) {
		if (op == FNCT_OP.Math_abs) {
			//abs() can have four different return types, depending on the parameters!
			return new QueryFunction(op, null, null, params[1].getReturnType(), null,  params);
		}
		return new QueryFunction(op, null, null, op.getReturnType(), null,  params);
	}
	
	public static QueryFunction createParam(QueryParameter param) {
		return new QueryFunction(FNCT_OP.PARAM, null, param, param.getType(), param.getTypeDef());
	}
	
	/**
	 * 
	 * @param currentInstance The current context for calling methods
	 * @param globalInstance The global context for 'this'
	 * @return Result of the evaluation
	 */
	Object evaluate(Object currentInstance, Object globalInstance) {
		switch (fnct) {
		case REF:
		case FIELD:
			try {
				Object localInstance = params[0].evaluate(currentInstance, globalInstance);
				if (localInstance == QueryTerm.NULL || localInstance == QueryTerm.INVALID) {
					return QueryTerm.INVALID;
				}
				if (localInstance instanceof ZooPC) {
					if (((ZooPC)localInstance).jdoZooIsStateHollow()) {
						((ZooPC)localInstance).zooActivateRead();
					}
				}
				if (field.getDeclaringClass() == localInstance.getClass()) {
					//Shortcut for base class, optimisation for non-polymorphism
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
		case PARAM: return ((QueryParameter)constant).getValue();
		case THIS: return globalInstance;
		default: return evaluateFunction(params[0].evaluate(currentInstance, globalInstance),
				globalInstance);
		}
	}

	private Object evaluateFunction(Object li, Object gi) {
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
		
		Object o1, o2;
		Object[] arg = new Object[params.length];
		for (int i = 1; i < arg.length; i++) {
			Object o = getValue(li, gi, i-1);
			if (o == QueryTerm.INVALID) {
				return QueryTerm.INVALID;
			}
			//TODO add flag to enum: allowsNullParams
			if (o == QueryTerm.NULL
					&& fnct!=FNCT_OP.LIST_get
					&& fnct!=FNCT_OP.MAP_get
					&& fnct!=FNCT_OP.EQ_OBJ) {
				return QueryTerm.INVALID;
			}
			arg[i-1] = o;
		}

		switch (fnct) {
		case COLL_contains: return ((Collection<?>)li).contains(arg[0]);
		case COLL_isEmpty: return ((Collection<?>)li).isEmpty();
		case COLL_size: return ((Collection<?>)li).size();
		case LIST_get: 
			int posL =(int)arg[0];
			int sizeL = ((List<?>)li).size();
			return (posL >= sizeL || posL < 0) ? QueryTerm.INVALID : ((List<?>)li).get(posL);
		case MAP_get: 
			return ((Map<?, ?>)li).get(arg[0]);
		case MAP_containsKey: return ((Map<?,?>)li).containsKey(arg[0]) ;
		case MAP_containsValue: return ((Map<?,?>)li).containsValue(arg[0]);
		case MAP_isEmpty: return ((Map<?,?>)li).isEmpty();
		case MAP_size: return ((Map<?,?>)li).size();
		case STR_startsWith: return ((String)li).startsWith((String) arg[0]);
		case STR_endsWith: return ((String)li).endsWith((String) arg[0]);
		case STR_matches: return ((String)li).matches((String) arg[0]);
		case STR_contains_NON_JDO: return ((String)li).contains((String) arg[0]);
		case STR_indexOf1: return ((String)li).indexOf((String) arg[0]);
		case STR_indexOf2: return ((String)li).indexOf(
				(String) arg[0], 
				(Integer) arg[1]);
		case STR_substring1: return ((String)li).substring((Integer) arg[0]);
		case STR_substring2: return ((String)li).substring(
				(Integer) arg[0], 
				(Integer) arg[1]);
		case STR_toLowerCase: return ((String)li).toLowerCase();
		case STR_toUpperCase: return ((String)li).toUpperCase();
		case STR_length: return ((String)li).length();
		case STR_trim: return ((String)li).trim();
		case Math_abs:
			Object o = arg[0];
			Class<?> oType = o.getClass();
			if (oType == Integer.class) {
				return Math.abs((Integer)o);
			} else if (oType == Long.class) {
				return Math.abs((Long)o);
			} else if (oType == Float.class) {
				return Math.abs((Float)o);
			} else if (oType == Double.class) {
				return Math.abs((Double)o);
			}
		case Math_sqrt:
			double d = toDouble(arg[0]);
			return d >= 0 ? Math.sqrt(d) : QueryTerm.INVALID;
		case Math_sin:
			return Math.sin(toDouble(arg[0]));
		case Math_cos:
			return Math.cos(toDouble(arg[0]));
		case EQ_NUM:
		case EQ_BOOL:
		case EQ_OBJ:
			o1 = arg[0];
			o2 = arg[1];
			if (o1 instanceof ZooPC) {
				long oid1 = ((ZooPC)o1).jdoZooGetOid();
				return (o2 instanceof ZooPC) ? ((ZooPC)o2).jdoZooGetOid() == oid1 : false;
			}
			return o1.equals(o2);
		case PLUS_STR:
			return ((String)arg[0]) + ((String)arg[1]);
		case PLUS_D:
			double d1 = TypeConverterTools.toDouble(arg[0]);
			double d2 = TypeConverterTools.toDouble(arg[1]);
			return d1 + d2;
		case PLUS_L:
			o1 = arg[0];
			o2 = arg[1];
			return TypeConverterTools.toLong(arg[0]) + TypeConverterTools.toLong(arg[1]);
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

	private Object getValue(Object localInstance, Object globalInstance, int i) {
		return params[i+1].evaluate(localInstance, globalInstance);
	}

	@Override
	public String toString() {
//		return fnct.name() + "[" + (field != null ? field.getName() : null) + 
//				"](" + Arrays.toString(params) + ")";
		return (field != null ? field.getName() : fnct.name()) 
				+ "(" + Arrays.toString(params) + ")";
	}

	public FNCT_OP op() {
		return fnct;
	}

	public Object getConstant() {
		return constant;
	}

	public QueryFunction[] getParams() {
		return params;
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
}
