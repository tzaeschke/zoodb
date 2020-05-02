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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Date;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.util.DBLogger;

public class TypeConverterTools {

	
	public enum COMPARISON_TYPE {
		SCO(true, Object.class),  //can be number, e.g. Long.class
		BIG_DECIMAL(true, BigDecimal.class),
		BIG_INT(true, BigInteger.class),
		DOUBLE(true, Double.class), 
		FLOAT(true, Float.class),
		LONG(true, Long.class),
		INT(true, Integer.class),
		SHORT(true, Short.class),
		BYTE(true, Byte.class),
		CHAR(true, Character.class),
		BOOLEAN(false, Boolean.class),
		STRING(false, String.class),
		DATE(false, Date.class),
		LOCAL_DATETIME(false, LocalDateTime.class),
		LOCAL_DATE(false, LocalDate.class),
		LOCAL_TIME(false, LocalTime.class),
		INSTANT(false, Instant.class),
		PERIOD(false, Period.class),
		DURATION(false, Duration.class),
		PC(false, ZooPC.class),
		//TODO move to TOP of list???!!!
		NULL(true, null),
		UNKNOWN(true, Object.class);
		private final boolean canBeNumber;
		private final Class<?> resultType;
		COMPARISON_TYPE(boolean canBeNumber, Class<?> resultType) {
			this.canBeNumber = canBeNumber;
			this.resultType = resultType;
		}
		public boolean canBeNumber() {
			return canBeNumber;
		}
		public Class<?> getResultType() {
			return resultType;
		}
		public static COMPARISON_TYPE fromObject(Object v) {
			if (v == null || v == QueryTerm.NULL) {
				return NULL;
			}
			return fromClass(v.getClass());
		}
		
		public static COMPARISON_TYPE fromClass(Class<?> type) {
			//TODO we can use perfect hashing here for fast lookup!
			if (type == null) {  //can happen for implicit parameters
				return UNKNOWN;
			} else if (type == Long.class || type == Long.TYPE) {
				return LONG;
			} else if (type == Integer.class || type == Integer.TYPE) {
				return INT;
			} else if (type == Short.class || type == Short.TYPE) {
				return SHORT;
			} else if (type == Byte.class || type == Byte.TYPE) {
				return BYTE;
			} else if (type == Double.class || type == Double.TYPE) {
				return DOUBLE;
			} else if (type == Float.class || type == Float.TYPE) {
				return FLOAT;
			} else if (type == Character.class || type == Character.TYPE) {
				return CHAR;
			} else if (type == String.class) {
				return STRING;
			} else if (type == Boolean.class || type == Boolean.TYPE) {
				return BOOLEAN;
			} else if (ZooPC.class.isAssignableFrom(type)) {
				return PC;
			} else if (type == Date.class) {
				return DATE;
			} else if (type == LocalDate.class) {
				return LOCAL_DATE;
			} else if (type == LocalTime.class) {
				return LOCAL_TIME;
			} else if (type == LocalDateTime.class) {
				return LOCAL_DATETIME;
			} else if (type == Instant.class) {
				return INSTANT;
			} else if (type == Period.class) {
				return PERIOD;
			} else if (type == Duration.class) {
				return DURATION;
			} else if (type == BigInteger.class) {
				return BIG_INT;
			} else if (type == BigDecimal.class) {
				return BIG_DECIMAL;
			}
			return SCO;
		}
		
		
		public static COMPARISON_TYPE fromOperands(COMPARISON_TYPE lhsCt,
				COMPARISON_TYPE rhsCt) {
			//swap them (according to ordinal()) to eliminate some 'if'. (?)
			if (rhsCt.ordinal() < lhsCt.ordinal()) {
				COMPARISON_TYPE x = lhsCt;
				lhsCt = rhsCt;
				rhsCt = x;
			}
			
			//see: https://docs.oracle.com/javase/specs/jls/se8/html/jls-5.html
			//     Section 5.6.2. Binary Numeric Promotion
			
			//- If either operand is of type double, the other is converted to double.
			//- Otherwise, if either operand is of type float, the other is converted to float.
			//- Otherwise, if either operand is of type long, the other is converted to long.
			//- Otherwise, both operands are converted to type int
			switch (lhsCt) {
			case SCO:
				return SCO;
			case BIG_DECIMAL:
			case BIG_INT: 
			case DOUBLE:
			case FLOAT:
			case LONG:
			case INT:
				if (rhsCt.canBeNumber()) {
					return lhsCt;
				} 
				failComp(lhsCt, rhsCt);
			case SHORT:
			case BYTE:
			case CHAR:
				if (rhsCt == CHAR) {
					return CHAR;
				} 
				if (rhsCt.canBeNumber()) {
					return INT;
				} 
				if (rhsCt == STRING) {
					return STRING;
				} 
				failComp(lhsCt, rhsCt);
			case BOOLEAN:
				if (rhsCt == BOOLEAN) {
					return BOOLEAN;
				}
				if (rhsCt == SCO) {
					return SCO;
				}
				failComp(lhsCt, rhsCt);
			case PC:
				if (rhsCt == PC || rhsCt == NULL) {
					return PC;
				}
				if (rhsCt == SCO) {
					return SCO;
				}
				//implicit parameters
				if (rhsCt == UNKNOWN) {
					return UNKNOWN;
				}
				failComp(lhsCt, rhsCt);
			case STRING:
				if (rhsCt == STRING || rhsCt == NULL) {
					return STRING;
				}
				//implicit parameters
				if (rhsCt == UNKNOWN) {
					return UNKNOWN;
				}
				failComp(lhsCt, rhsCt);
			default: return UNKNOWN;
			}
		}

		private static void failComp(COMPARISON_TYPE lhsCt,
				COMPARISON_TYPE rhsCt) {
			throw DBLogger.newUser("Incompatible types: Cannot compare " + lhsCt + " with " + rhsCt);
		}
	}

	public static COMPARISON_TYPE fromTypes(Class<?> lhs, Class<?> rhs) {
		COMPARISON_TYPE lhsCt = COMPARISON_TYPE.fromClass(lhs);
		COMPARISON_TYPE rhsCt = COMPARISON_TYPE.fromClass(rhs);
		return COMPARISON_TYPE.fromOperands(lhsCt, rhsCt);
	}
	
	/**
	 * This assumes that comparability implies assignability or convertibility...
	 * @param o object
	 * @param type type
	 */
	public static void checkAssignability(Object o, Class<?> type) {
		COMPARISON_TYPE ctO = COMPARISON_TYPE.fromObject(o);
		COMPARISON_TYPE ctT = COMPARISON_TYPE.fromClass(type);
		try {
			COMPARISON_TYPE.fromOperands(ctO, ctT);
		} catch (Exception e) {
			throw DBLogger.newUser("Cannot assign " + o.getClass() + " to " + type, e);
		}
	}
	
	/**
	 * This assumes that comparability implies assignability or convertibility...
	 * @param c1 type #1
	 * @param c2 type #2
	 */
	public static void checkAssignability(Class<?> c1, Class<?> c2) {
		COMPARISON_TYPE ct1 = COMPARISON_TYPE.fromClass(c1);
		COMPARISON_TYPE ct2 = COMPARISON_TYPE.fromClass(c2);
		try {
			COMPARISON_TYPE.fromOperands(ct1, ct2);
		} catch (Exception e) {
			throw DBLogger.newUser("Cannot assign " + c2 + " to " + c1, e);
		}
	}
	
	public static double toDouble(Object o) { 
		if (o instanceof Double) {
			return (double)o; 
		} else if (o instanceof Float) {
			return (float)o; 
		} 
		return toLong(o);
	}
	
	public static float toFloat(Object o) { 
		if (o instanceof Float) {
			return (float)o; 
		} 
		return toInt(o);
	}
	
	public static long toLong(Object o) { 
		if (o instanceof Long) {
			return (long)o; 
		}
		return toInt(o);
	}

	public static int toInt(Object o) {
		//TODO use perfect hashing on types to create hashmap?
		if (o instanceof Integer) {
			return (int)(Integer)o;
		} else if (o instanceof Short) {
			return (Short)o;
		} else if (o instanceof Byte) {
			return (Byte)o;
		} else if (o instanceof Character) {
			return (Character)o;
		}
		throw DBLogger.newUser("Cannot cast type to number: " + o.getClass().getName());
	}
	
	public static String convertToString(Object o) {
		if (o instanceof String) {
			return (String)o;
		} else if (o instanceof Character) {
			return String.valueOf(o);
		}
		throw DBLogger.newUser("Cannot cast type to String: " + o.getClass().getName());
	}
	
	
}
