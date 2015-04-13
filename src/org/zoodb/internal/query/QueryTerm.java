/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
import java.util.Collection;
import java.util.Map;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.DataDeSerializerNoClass;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryParser.COMP_OP;
import org.zoodb.internal.util.DBLogger;

public final class QueryTerm {

	private final ZooFieldDef lhsFieldDef;
	private final COMP_OP op;
	private final String rhsParamName;
	private final Object rhsValue;
	private QueryParameter rhsParam;
	private final ZooFieldDef rhsFieldDef;
	
	public QueryTerm(ZooFieldDef lhsFieldDef, COMP_OP op, String rhsParamName,
			Object rhsValue, ZooFieldDef rhsFieldDef, boolean negate) {
		this.lhsFieldDef = lhsFieldDef;
		this.op = op.inverstIfTrue(negate);
		this.rhsParamName = rhsParamName;
		this.rhsValue = rhsValue;
		this.rhsFieldDef = rhsFieldDef;
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
	
	public Object getValue(Object o) {
		if (rhsParamName != null) {
			return rhsParam.getValue();
		}
		if (rhsFieldDef != null) {
			try {
				return rhsFieldDef.getJavaField().get(o);
			} catch (IllegalArgumentException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + rhsFieldDef.getName() + 
						" class=\"" + o.getClass().getName() + "\"," + 
						" declaring class=\"" + rhsFieldDef.getDeclaringType().getClassName() + 
						"\"", e);
			} catch (IllegalAccessException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + rhsFieldDef.getName(), e);
			}
		}
		return rhsValue;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean evaluate(Object cand) {
		// we cannot cache this, because sub-classes may have different field instances.
		//TODO cache per class? Or reset after query has processed first class set?
		Field lhsField = lhsFieldDef.getJavaField();

		Object lhsVal;
		try {
			lhsVal = lhsField.get(cand);
		} catch (IllegalArgumentException e) {
			throw DBLogger.newFatalInternal("Cannot access field: " + lhsField.getName() + 
					" class=\"" + cand.getClass().getName() + "\"," + 
					" declaring class=\"" + lhsField.getDeclaringClass().getName()+ "\"", e);
		} catch (IllegalAccessException e) {
			throw DBLogger.newFatalInternal("Cannot access field: " + lhsField.getName(), e);
		}
		
		if (!op.isComparator()) {
			return evaluateBoolFunction(lhsVal, cand);
		}
		
		//TODO avoid indirection and store Parameter value in local _value field !!!!!!!!!!!!!!!!
		Object qVal = getValue(cand);
		if (lhsVal == null) {
			if (qVal == QueryParser.NULL && (op==COMP_OP.EQ || op==COMP_OP.LE || op==COMP_OP.AE)) {
				return true;
			}
			//ordering of null-value fields is not specified (JDO 3.0 14.6.6: Ordering Statement)
			//We specify:  (null <= 'x' ==true)
			if (qVal != QueryParser.NULL && (op==COMP_OP.NE || op==COMP_OP.LE || op==COMP_OP.L)) {
				return true;
			}
		} else if (qVal != QueryParser.NULL && lhsVal != null) {
			if (qVal.equals(lhsVal) && (op==COMP_OP.EQ || op==COMP_OP.LE || op==COMP_OP.AE)) {
				return true;
			}
			if (qVal instanceof Comparable) {
				Comparable qComp = (Comparable) qVal;
				int res = qComp.compareTo(lhsVal);  //-1:<   0:==  1:> 
				if (res >= 1 && (op == COMP_OP.LE || op==COMP_OP.L || op==COMP_OP.NE)) {
					return true;
				} else if (res <= -1 && (op == COMP_OP.AE || op==COMP_OP.A || op==COMP_OP.NE)) {
					return true;
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

	private boolean evaluateBoolFunction(Object rhsVal, Object cand) {
		if (rhsVal == null) {
			//According to JDO spec 14.6.2, calls on 'null' result in 'false'
			return false;
		}
		switch (op) {
		case COLL_contains: return ((Collection<?>)rhsVal).contains(getValue(cand));
		case COLL_isEmpty: return ((Collection<?>)rhsVal).isEmpty();
		case MAP_containsKey: return ((Map<?,?>)rhsVal).containsKey(getValue(cand)) ;
		case MAP_containsValue: return ((Map<?,?>)rhsVal).containsValue(getValue(cand));
		case MAP_isEmpty: return ((Map<?,?>)rhsVal).isEmpty();
		case STR_startsWith: return ((String)rhsVal).startsWith((String) getValue(cand));
		case STR_endsWith: return ((String)rhsVal).endsWith((String) getValue(cand));
		case STR_matches: return ((String)rhsVal).matches((String) getValue(cand));
		case STR_contains_NON_JDO: return ((String)rhsVal).contains((String) getValue(cand));
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
		if (qVal != QueryParser.NULL) {
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
		sb.append(lhsFieldDef.getName());
		sb.append(" ");
		sb.append(op);
		sb.append(" ");
		sb.append(rhsValue);
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
	
}