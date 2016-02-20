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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.DataDeSerializerNoClass;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryParser.COMP_OP;
import org.zoodb.internal.util.DBLogger;

import static org.zoodb.internal.query.TypeConverterTools.*;

public final class QueryTerm {

	static final Object THIS = new Object();
	static final Object NULL = new NullClass();
	/** So NULL has a different type than other objects. */
	private static final class NullClass{};

	/** Represent result from evaluating functions on references that are 'null'. */
	static final Object INVALID = new InvalidClass();
	/** So INVALID has a different type than other objects. */
	private static final class InvalidClass{};

	private QueryParameter lhsParam;
	private final Object lhsValue;
	private final ZooFieldDef lhsFieldDef;
	private final QueryFunction lhsFunction;
	private final COMP_OP op;
	private final String rhsParamName;
	private final Object rhsValue;
	private QueryParameter rhsParam;
	private final ZooFieldDef rhsFieldDef;
	private final QueryFunction rhsFunction;
	
	//Even if the COMPARISON type is not known, it may be constant (type in collection or similar).
	//That means we could create an execution branch that assumes the comparison type of the 
	//previous execution, or the previous few executions.
	private final COMPARISON_TYPE lhsCt;
	private final COMPARISON_TYPE rhsCt;
	private final COMPARISON_TYPE compType;
	
	
	public QueryTerm(QueryFunction lhsFunction, boolean negate) {
		this.lhsParam = null;
		switch (lhsFunction.op()) {
		case THIS:
			this.lhsFunction = null;
			this.lhsValue = THIS;
			break;
		case CONSTANT:
			this.lhsFunction = null;
			this.lhsValue = lhsFunction.getConstant();
			break;
		case PARAM:
			this.lhsFunction = null;
			this.lhsValue = null;
			this.lhsParam = (QueryParameter) lhsFunction.getConstant();
			break;
		default:
			this.lhsFunction = lhsFunction;
			this.lhsValue = null;
		}
		this.lhsFieldDef = null;
		this.op = COMP_OP.EQ.inverstIfTrue(negate);
		this.rhsParamName = null;
		this.rhsValue = true;
		this.rhsFieldDef = null;
		this.rhsFunction = null;

		this.lhsCt = COMPARISON_TYPE.BOOLEAN; //Hmm, that could be anything, no?
		this.rhsCt = COMPARISON_TYPE.BOOLEAN;
		this.compType = COMPARISON_TYPE.BOOLEAN;
	}

	public QueryTerm(Object lhsValue, ZooFieldDef lhsFieldDef, 
			QueryFunction lhsFunction, 
			COMP_OP op, String rhsParamName,
			Object rhsValue, ZooFieldDef rhsFieldDef, 
			QueryFunction rhsFunction, boolean negate) {
		//LHS
		this.lhsParam = null; 
		if (lhsFunction != null) {
			switch (lhsFunction.op()) {
			case THIS:
				this.lhsFunction = null;
				this.lhsValue = THIS;
				this.lhsCt = COMPARISON_TYPE.PC;
				break;
			case CONSTANT:
				this.lhsFunction = null;
				this.lhsValue = lhsFunction.evaluate(null,  null);
				this.lhsCt = COMPARISON_TYPE.fromType(lhsValue);
				break;
			case PARAM:
				this.lhsFunction = null;
				this.lhsValue = null;
				this.lhsParam = (QueryParameter) lhsFunction.getConstant();
				this.lhsCt = COMPARISON_TYPE.fromType(lhsParam.getValue()); //may be null 
				break;
			default:
				this.lhsFunction = lhsFunction;
				this.lhsValue = null;
				this.lhsCt = COMPARISON_TYPE.fromTypeClass(lhsFunction.getReturnType()); 
			}
		} else {
			this.lhsFunction = lhsFunction;
			this.lhsValue = lhsValue;
			if (lhsFieldDef != null) {
				this.lhsCt = COMPARISON_TYPE.fromTypeClass(lhsFieldDef.getJavaType());
			} else {
				this.lhsCt = COMPARISON_TYPE.fromType(lhsValue);
			}
		}
		this.lhsFieldDef = lhsFieldDef;
		
		//OP
		this.op = op.inverstIfTrue(negate);
		
		//RHS
		this.rhsParamName = rhsParamName;
		this.rhsFieldDef = rhsFieldDef;
		if (rhsFunction != null) {
			switch (rhsFunction.op()) {
			case THIS:
				this.rhsFunction = null;
				this.rhsValue = THIS;
				this.rhsCt = COMPARISON_TYPE.PC;
				break;
			case CONSTANT:
				this.rhsFunction = null;
				this.rhsValue = rhsFunction.evaluate(null,  null);
				this.rhsCt = COMPARISON_TYPE.fromType(rhsValue);
				break;
			case PARAM:
				this.rhsFunction = null;
				this.rhsValue = null;
				this.rhsParam = (QueryParameter) rhsFunction.getConstant();
				this.rhsCt = COMPARISON_TYPE.fromType(rhsParam.getValue()); //may be null 
				break;
			default:
				this.rhsFunction = rhsFunction;
				this.rhsValue = null;
				this.rhsCt = COMPARISON_TYPE.fromTypeClass(rhsFunction.getReturnType()); 
			}
		} else {
			this.rhsFunction = rhsFunction;
			this.rhsValue = rhsValue;
			if (rhsFieldDef != null) {
				this.rhsCt = COMPARISON_TYPE.fromTypeClass(rhsFieldDef.getJavaType());
			} else {
				this.rhsCt = COMPARISON_TYPE.fromType(rhsValue);
			}
		}
		
		compType = COMPARISON_TYPE.fromOperands(lhsCt, rhsCt);
	}

	public boolean isParametrized() {
		return rhsParamName != null;
	}

	public String getParamName() {
		return rhsParamName;
	}

	public void setParameter(QueryParameter param) {
		this.rhsParam = param;
	}
	
	public QueryParameter getParameter() {
		return rhsParam;
	}
	
	public Object getValue(Object cand) {
		if (rhsFunction != null) {
			return rhsFunction.evaluate(cand, cand);
		} else if (rhsParam != null) {
			return rhsParam.getValue();
		}
		if (rhsFieldDef != null) {
			try {
				return rhsFieldDef.getJavaField().get(cand);
			} catch (IllegalArgumentException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + rhsFieldDef.getName() + 
						" class=\"" + cand.getClass().getName() + "\"," + 
						" declaring class=\"" + rhsFieldDef.getDeclaringType().getClassName() + 
						"\"", e);
			} catch (IllegalAccessException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + rhsFieldDef.getName(), e);
			}
		}
		if (rhsValue == THIS) {
			return cand;
		}
		return rhsValue;
	}

	private Object getLhsValue(Object cand) {
		if (lhsFunction != null) {
			return lhsFunction.evaluate(cand, cand);
		} else if (lhsValue == THIS){
			return cand;
		} else if (lhsValue != null){
			return lhsValue;
		} else if (lhsParam != null) {
			return lhsParam.getValue();
		} else {
			// we cannot cache this, because sub-classes may have different field instances.
			//TODO cache per class? Or reset after query has processed first class set?
			Field lhsField = lhsFieldDef.getJavaField();
	
			try {
				return lhsField.get(cand);
			} catch (IllegalArgumentException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + lhsField.getName() + 
						" class=\"" + cand.getClass().getName() + "\"," + 
						" declaring class=\"" + lhsField.getDeclaringClass().getName()+ "\"", e);
			} catch (IllegalAccessException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + lhsField.getName(), e);
			}
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean evaluate(Object cand) {
		Object lhsVal = getLhsValue(cand);
		if (lhsVal == INVALID) {
			//Return 'true' only in case of '!='. 
			return false;//op == COMP_OP.NE;
		}
		
		if (!op.isComparator()) {
			return evaluateBoolFunction(lhsVal, cand);
		}
		
		//TODO avoid indirection and store Parameter value in local _value field !!!!!!!!!!!!!!!!
		Object qVal = getValue(cand);
		
		switch (compType) {
		//TODO implement op.compare(int, int) directly
		//TODO convert constants only once and store converted values...
		//TODO write dedicated converters for <int,int>, <byte,byte>, ... to avoid type conversion
		case FLOAT:
		case DOUBLE:
			return op.evaluate(Double.compare(toDouble(lhsVal), toDouble(qVal)));
		case CHAR:
		case BYTE:
		case SHORT:
		case INT:
		case LONG: 
			return op.evaluate(Long.compare(toLong(lhsVal), toLong(qVal)));
		default:
			break;
		}
		
		
		if (lhsVal == null) {
			if (qVal == QueryTerm.NULL && op.allowsEqual()) {
				return true;
			}
			//ordering of null-value fields is not specified (JDO 3.0 14.6.6: Ordering Statement)
			//We specify:  (null <= 'x' ==true)
			if (qVal != QueryTerm.NULL && op.allowsLess()) {
				return true;
			}
		} else if (lhsVal instanceof ZooPC && qVal instanceof ZooPC) {
			long oid1 = ((ZooPC)lhsVal).jdoZooGetOid();
			long oid2 = ((ZooPC)qVal).jdoZooGetOid();
			if (oid1 == oid2 && op.allowsEqual()) {
				return true;
			} else if (op == COMP_OP.EQ) {
				return false; //shortcut for most common case: oid1==oid2
			}
			int res = Long.compare(oid2, oid1);
			if (res >= 1 && op.allowsLess()) {
				return true;
			} else if (res <= -1 && op.allowsMore()) {
				return true;
			}
		} else if (lhsVal instanceof ZooPC || qVal instanceof ZooPC) {
			//Either one of them is null or one of them is not a PC
			return op.allowsLess() || op.allowsMore();
		} else if (qVal != QueryTerm.NULL && lhsVal != null) {
			if (qVal.equals(lhsVal) && op.allowsEqual()) {
				return true;
			} else if (op == COMP_OP.EQ) {
				return false; //shortcut for most common case: a==b
			}
			if (qVal instanceof Comparable) {
				Comparable qComp = (Comparable) qVal;
				try {
					int res = qComp.compareTo(lhsVal);  //-1:<   0:==  1:> 
					if (res >= 1 && op.allowsLess()) {
						return true;
					} else if (res <= -1 && op.allowsMore()) {
						return true;
					}
				} catch (ClassCastException e) {
					if ((lhsParam != null && lhsParam.getType() == null) ||
							(rhsParam != null && rhsParam.getType() == null)) {
						throw DBLogger.newUser("Incomparable types in implicit parameters: "
								+ lhsVal.getClass() + " vs " + qVal.getClass());
					}
					//this should not happen
					throw e;
				}
			}
		} else {
			//here: qVal == QueryParser.NULL && oVal != null
			//Ordering of null-value fields is not specified (JDO 3.0 14.6.6: Ordering Statement)
			//We specify:  (null <= 'x' ==true)
			if (op==COMP_OP.NE || op==COMP_OP.A || op==COMP_OP.AE) {
				return true;
			}
		}
		return false;
	}

	private boolean evaluateBoolFunction(Object lhsVal, Object cand) {
		if (lhsVal == null) {
			//According to JDO spec 14.6.2, calls on 'null' result in 'false'
			return false;
		}
		switch (op) {
		case COLL_contains: return ((Collection<?>)lhsVal).contains(getValue(cand));
		case COLL_isEmpty: return ((Collection<?>)lhsVal).isEmpty();
		case MAP_containsKey: return ((Map<?,?>)lhsVal).containsKey(getValue(cand)) ;
		case MAP_containsValue: return ((Map<?,?>)lhsVal).containsValue(getValue(cand));
		case MAP_isEmpty: return ((Map<?,?>)lhsVal).isEmpty();
		case STR_startsWith: return ((String)lhsVal).startsWith((String) getValue(cand));
		case STR_endsWith: return ((String)lhsVal).endsWith((String) getValue(cand));
		case STR_matches: return ((String)lhsVal).matches((String) getValue(cand));
		case STR_contains_NON_JDO: return ((String)lhsVal).contains((String) getValue(cand));
		default:
			throw new UnsupportedOperationException(op.name());
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean evaluate(DataDeSerializerNoClass dds, long pos) {
		// we cannot cache this, because sub-classes may have different field instances.
		//TODO cache per class? Or reset after query has processed first class set?
		//Field f = fieldDef.getJavaField();

		long oVal;
//		try {
			dds.seekPos(pos);
			//TODO this doesn't really work for float/double
			oVal = dds.getAttrAsLong(lhsFieldDef.getDeclaringType(), lhsFieldDef);
//		} catch (IllegalArgumentException e) {
//			throw new JDOFatalInternalException("Cannot access field: " + fieldDef.getName() + 
//					" cl=" + fieldDef.getDeclaringType().getClassName() + 
//					" fcl=" + f.getDeclaringClass().getName(), e);
//		}
		//TODO avoid indirection and store Parameter value in local _value field !!!!!!!!!!!!!!!!
		//TODO using 'null' for now, knowing it won't work...
		Object qVal = getValue(null);
		if (qVal != QueryTerm.NULL) {
			if (qVal.equals(oVal) && (op==COMP_OP.EQ || op==COMP_OP.LE || op==COMP_OP.AE)) {
				return true;
			}
			if (qVal instanceof Comparable) {
				Comparable qComp = (Comparable) qVal;
				int res = qComp.compareTo(oVal);  //-1:<   0:==  1:> 
				if (res == 1 && (op == COMP_OP.LE || op==COMP_OP.L || op==COMP_OP.NE)) {
					return true;
				} else if (res == -1 && (op == COMP_OP.AE || op==COMP_OP.A || op==COMP_OP.NE)) {
					return true;
				}
			}
		}
		return false;
	}

	public String print() {
		StringBuilder sb = new StringBuilder();
		if (lhsFieldDef != null) {
			sb.append(lhsFieldDef.getName());
		} else if (lhsValue != null) {
			sb.append(lhsValue);
		} else if (lhsParam != null) {
			sb.append("P-" + lhsParam.getName());
		} else if (lhsFunction != null) {
			sb.append(lhsFunction.toString());
		} else {
			sb.append("???");
		}
		sb.append(" ");
		sb.append(op);
		sb.append(" ");
		if (rhsFieldDef != null) {
			sb.append(rhsFieldDef.getName());
		} else if (rhsValue != null) {
			sb.append(rhsValue);
		} else if (rhsParam != null) {
			sb.append("P-" + rhsParam.getName());
		} else if (rhsFunction != null) {
			sb.append(rhsFunction.toString());
		} else {
			sb.append("???");
		}
		return sb.toString();
	}

	ZooFieldDef getLhsFieldDef() {
		return lhsFieldDef;
	}

	COMP_OP getOp() {
		return op;
	}

	boolean isRhsFixed() {
		return rhsFieldDef == null;
	}
	
	public boolean isLhsFunction() {
		return lhsFunction != null;
	}

	public QueryFunction getLhsFunction() {
		return lhsFunction;
	}
}