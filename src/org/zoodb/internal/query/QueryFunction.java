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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryParser.FNCT_OP;

/**
 * Query functions.
 * 
 * @author ztilmann
 *
 */
public class QueryFunction {

	private final Class<?> returnType;
	private final FNCT_OP fnct;
	private final int fieldId;
	private final Field field;
	private final ZooFieldDef zField;
	private final QueryFunction[] params;
	private final Object constant;
	private boolean isConstant; //indicate whether evaluation is constant
	
	private QueryFunction(FNCT_OP fnct, ZooFieldDef zField, 
			Object constant, Class<?> returnType,
			QueryFunction ... params) {
		this.returnType = returnType;
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
		return new QueryFunction(FNCT_OP.CONSTANT, null, constant, constant.getClass());
	}
	
	public static QueryFunction createThis(Class<?> baseClass) {
		return new QueryFunction(FNCT_OP.THIS, null, null, baseClass);
	}
	
	public static QueryFunction createFieldRef(QueryFunction baseObjectFn, ZooFieldDef field) {
		return new QueryFunction(FNCT_OP.REF, field, null, field.getJavaType(), baseObjectFn);
	}
	
	public static QueryFunction createFieldSCO(QueryFunction baseObjectFn, ZooFieldDef field) {
		return new QueryFunction(FNCT_OP.FIELD, field, null, field.getJavaType(), baseObjectFn);
	}
	
	public static QueryFunction createJava(FNCT_OP op, QueryFunction ... params) {
		return new QueryFunction(op, null, null, op.getReturnType(), params);
	}
	
	public static QueryFunction createParam(QueryParameter param) {
		return new QueryFunction(FNCT_OP.PARAM, null, param, null);
	}
	
	/**
	 * 
	 * @param currentInstance The current context for calling methods
	 * @param globalInstance The global context for 'this'
	 * @return
	 */
	Object evaluate(Object currentInstance, Object globalInstance) {
		switch (fnct) {
		case REF:
		case FIELD:
			try {
				Object localInstance = params[0].evaluate(currentInstance, globalInstance);
				if (localInstance == null || localInstance == QueryTerm.INVALID) {
					return QueryTerm.INVALID;
				}
				if (localInstance instanceof ZooPC) {
					if (((ZooPC)localInstance).jdoZooIsStateHollow()) {
						((ZooPC)localInstance).zooActivateRead();
					}
				}
				if (field.getDeclaringClass() == localInstance.getClass()) {
					//Shortcut for base class, optimisation for non-polymorphism
					return field.get(localInstance);
				}
				//TODO why don't we need this in QueryTerm.evaluate()????
				ZooClassDef def = ((ZooPC)localInstance).jdoZooGetClassDef();
				return def.getAllFields()[fieldId].getJavaField().get(localInstance);
			} catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
				throw new RuntimeException(e);
			}
		case CONSTANT: return constant;
		case PARAM: return ((QueryParameter)constant).getValue();
		case THIS: return globalInstance;
		default: return evaluateBoolFunction(params[0].evaluate(currentInstance, globalInstance),
				globalInstance);
		}
	}

	private boolean evaluateBoolFunction(Object li, Object gi) {
		if (li == null || li == QueryTerm.INVALID) {
			//According to JDO spec 14.6.2, calls on 'null' result in 'false'
			return false;
		}
		switch (fnct) {
		case COLL_contains: return ((Collection<?>)li).contains(getValue(li, gi, 0));
		case COLL_isEmpty: return ((Collection<?>)li).isEmpty();
		case MAP_containsKey: return ((Map<?,?>)li).containsKey(getValue(li, gi, 0)) ;
		case MAP_containsValue: return ((Map<?,?>)li).containsValue(getValue(li, gi, 0));
		case MAP_isEmpty: return ((Map<?,?>)li).isEmpty();
		case STR_startsWith: return ((String)li).startsWith((String) getValue(li, gi, 0));
		case STR_endsWith: return ((String)li).endsWith((String) getValue(li, gi, 0));
		case STR_matches: return ((String)li).matches((String) getValue(li, gi, 0));
		case STR_contains_NON_JDO: return ((String)li).contains((String) getValue(li, gi, 0));
		default:
			throw new UnsupportedOperationException(fnct.name());
		}
	}

	private Object getValue(Object localInstance, Object globalInstance, int i) {
		return params[i+1].evaluate(localInstance, globalInstance);
	}

	@Override
	public String toString() {
//		return fnct.name() + "[" + (field != null ? field.getName() : null) + 
//				"](" + Arrays.toString(params) + ")";
		return (field != null ? field.getName() : fnct.name()) + "(" + Arrays.toString(params) + ")";
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
}
