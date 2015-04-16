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

import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.query.QueryParser.FNCT_OP;

/**
 * Query functions.
 * 
 * @author ztilmann
 *
 */
public abstract class QueryFunction {

	protected QueryFunction inner;
	private final Class<?> returnType;
	
	private QueryFunction(Class<?> returnType) {
		this.returnType = returnType;
	}
	
	abstract Object evaluate(Object instance);
	
	public Class<?> getReturnType() {
		return returnType;
	}

	public void setInner(QueryFunction fn) {
		if (inner != null) {
			throw new IllegalStateException();
		}
		inner = fn;
	}
	
	public static QueryFunction createConstant(Object constant) {
		//return new Constant(constant);
		return new Generic(FNCT_OP.CONSTANT, null, null, null, constant, constant.getClass());
	}
	
	public static QueryFunction createThis(Class<?> baseClass) {
		//return new Constant(constant);
		return new Generic(FNCT_OP.THIS, null, null, null, null, baseClass);
	}
	
	public static QueryFunction createFieldRef(QueryFunction baseObjectFn, Class<?> fieldType) {
		//return new Path(refField.getName(), refField, refField.getJavaField());
		return new Generic(FNCT_OP.REF, null, null, null, null, fieldType, baseObjectFn);
	}
	
	public static QueryFunction createFieldSCO(QueryFunction baseObjectFn, Class<?> fieldType) {
		//return new Path(refField.getName(), refField, refField.getJavaField());
		return new Generic(FNCT_OP.FIELD, null, null, null, null, fieldType, baseObjectFn);
	}
	
	public static QueryFunction createJava(FNCT_OP op, QueryFunction ... params) {
		return new Generic(op, null, null, null, null, op.getReturnType(), params);
	}
	
	public static class Generic extends QueryFunction {
		
		private final FNCT_OP fnct;
		private final String fieldName;
		private final ZooFieldDef zField;
		private final Field field;
		private final QueryFunction[] params;
		private final Object constant;
		
		public Generic(FNCT_OP fnct, String fieldName, ZooFieldDef zField, Field field, 
				Object constant, Class<?> returnType,
				QueryFunction ... params) {
			super(returnType);
			this.fnct = fnct;
			this.fieldName = fieldName;
			this.zField = zField;
			this.field = field;
			this.constant = constant;
			this.params = params;
		}

		@Override
		Object evaluate(Object instance) {
			switch (fnct) {
			case REF:
			case FIELD:
				try {
					if (inner == null) {
						return field.get(params[0]);
					} else {
						return inner.evaluate(field.get(params[0]));
					}
				} catch (IllegalArgumentException | IllegalAccessException e) {
					throw new RuntimeException(e);
				}
			case CONSTANT: return constant;
			case THIS: return instance;
			default: return evaluateBoolFunction(instance);
			}
		}
		
		private boolean evaluateBoolFunction(Object baseVal) {
			if (baseVal == null) {
				//According to JDO spec 14.6.2, calls on 'null' result in 'false'
				return false;
			}
			switch (fnct) {
			case COLL_contains: return ((Collection<?>)baseVal).contains(getValue(baseVal, 0));
			case COLL_isEmpty: return ((Collection<?>)baseVal).isEmpty();
			case MAP_containsKey: return ((Map<?,?>)baseVal).containsKey(getValue(baseVal, 0)) ;
			case MAP_containsValue: return ((Map<?,?>)baseVal).containsValue(getValue(baseVal, 0));
			case MAP_isEmpty: return ((Map<?,?>)baseVal).isEmpty();
			case STR_startsWith: return ((String)baseVal).startsWith((String) getValue(baseVal, 0));
			case STR_endsWith: return ((String)baseVal).endsWith((String) getValue(baseVal, 0));
			case STR_matches: return ((String)baseVal).matches((String) getValue(baseVal, 0));
			case STR_contains_NON_JDO: return ((String)baseVal).contains((String) getValue(baseVal, 0));
			default:
				throw new UnsupportedOperationException(fnct.name());
			}
		}

		private Object getValue(Object baseVal, int i) {
			return params[i].evaluate(baseVal);
		}
	}

	public static class Path extends QueryFunction {
		
		private final String fieldName;
		private final ZooFieldDef zField;
		private final Field field;
		
		
		
		
		public Path(String fieldName, ZooFieldDef zField, Field field) {
			super(null);
			this.fieldName = fieldName;
			this.zField = zField;
			this.field = field;
		}

		@Override
		Object evaluate(Object instance) {
			try {
				if (inner == null) {
					return field.get(instance);
				} else {
					return inner.evaluate(field.get(instance));
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		
	}

	public static class Java extends QueryFunction {
		
		private final FNCT_OP fnct;
		private final QueryFunction[] params;
		
		public Java(FNCT_OP fnct, QueryFunction ... params) {
			super(null);
			this.fnct = fnct;
			this.params = params;
		}

		@Override
		Object evaluate(Object instance) {
			return evaluateBoolFunction(instance);
		}
		
		private boolean evaluateBoolFunction(Object baseVal) {
			if (baseVal == null) {
				//According to JDO spec 14.6.2, calls on 'null' result in 'false'
				return false;
			}
			switch (fnct) {
			case COLL_contains: return ((Collection<?>)baseVal).contains(getValue(baseVal, 0));
			case COLL_isEmpty: return ((Collection<?>)baseVal).isEmpty();
			case MAP_containsKey: return ((Map<?,?>)baseVal).containsKey(getValue(baseVal, 0)) ;
			case MAP_containsValue: return ((Map<?,?>)baseVal).containsValue(getValue(baseVal, 0));
			case MAP_isEmpty: return ((Map<?,?>)baseVal).isEmpty();
			case STR_startsWith: return ((String)baseVal).startsWith((String) getValue(baseVal, 0));
			case STR_endsWith: return ((String)baseVal).endsWith((String) getValue(baseVal, 0));
			case STR_matches: return ((String)baseVal).matches((String) getValue(baseVal, 0));
			case STR_contains_NON_JDO: return ((String)baseVal).contains((String) getValue(baseVal, 0));
			default:
				throw new UnsupportedOperationException(fnct.name());
			}
		}

		private Object getValue(Object baseVal, int i) {
			return params[i].evaluate(baseVal);
		}
	}

	public static class FieldReader extends QueryFunction {
		
		private final Field field;
		
		public FieldReader(Field field) {
			super(null);
			this.field = field;
		}

		@Override
		Object evaluate(Object instance) {
			try {
				if (inner == null) {
					return field.get(instance);
				} else {
					return inner.evaluate(field.get(instance));
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		
	}


	public static class Constant extends QueryFunction {
		
		private final Object value;
		
		public Constant(Object value) {
			super(null);
			this.value = value;
		}

		@Override
		Object evaluate(Object instance) {
			if (instance != null) {
				//TODO remove?
				throw new IllegalArgumentException();
			}
			return value;
		}
		
	}
}
