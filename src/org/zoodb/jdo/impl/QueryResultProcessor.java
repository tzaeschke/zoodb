/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;

import org.zoodb.internal.SerializerTools.PRIMITIVE;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.util.DBLogger;

/**
 * Processes query results.
 * 
 * See Jdo 2.2 spec 14.6.9.
 * 
 * @author Tilmann Zaeschke
 *
 */
class QueryResultProcessor {

	private final ArrayList<Item> items = new ArrayList<Item>();
	private boolean isProjection = false;
	
	
	private static abstract class Item {
		ZooFieldDef field;
		Field jField;
		Class<?> resultClass;
		boolean isFloat;
		void setField(ZooFieldDef field, Class<?> resultClass) {
			this.field = field;
			if (field.getJavaField() == null) {
				field.getDeclaringType().associateJavaTypes();
			}
			this.jField = field.getJavaField();
			this.resultClass = resultClass; 
			if (resultClass == null) {
				this.resultClass = jField.getType();
			}
			isFloat = field.getPrimitiveType() == PRIMITIVE.FLOAT || 
					field.getPrimitiveType() == PRIMITIVE.DOUBLE;
		}
		protected Object getValue(Object o) {
			try {
				return jField.get(o);
			} catch (IllegalArgumentException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		protected double getFloat(Object o) {
			try {
				switch (field.getPrimitiveType()) {
				case DOUBLE: return jField.getDouble(o);
				case FLOAT: return jField.getFloat(o);
				default:
					throw new UnsupportedOperationException(field.getPrimitiveType().name());
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		protected long getInt(Object o) {
			try {
				switch (field.getPrimitiveType()) {
				case BYTE: return jField.getByte(o);
				case CHAR: return jField.getChar(o);
				case INT: return jField.getInt(o);
				case LONG: return jField.getLong(o);
				case SHORT: return jField.getShort(o);
				default:
					throw new UnsupportedOperationException(field.getPrimitiveType().name());
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		Object toFloat(double d) {
			switch (field.getPrimitiveType()) {
	    	case DOUBLE: return (double)d;
	    	case FLOAT: return (float)d;
	    	default:
	    		throw new UnsupportedOperationException(field.getPrimitiveType().name());
	    	}
		}
		Object toInt(long l) {
			switch (field.getPrimitiveType()) {
	    	//case BOOLEAN: return (boolean)avg;
	    	case BYTE: return (byte)l;
	    	case CHAR: return (char)l;
	    	case DOUBLE: return (double)l;
	    	case FLOAT: return (float)l;
	    	case INT: return (int)l;
	    	case LONG: return (long)l;
	    	case SHORT: return(short)l;
	    	default:
	    		throw new UnsupportedOperationException(field.getPrimitiveType().name());
			}
		}
		abstract void add(Object o);
		abstract Object result();
	}
	
	private static class AVG extends Item {
		private double d;
		private long l;
		long n;
		@Override
		void add(Object o) {
			n++;
			if (isFloat) {
				d += getFloat(o);
			} else {
				l += getInt(o);
			}
		}
		@Override
		Object result() {
			if (isFloat) {
				double avg = d/(double)n;
				return toFloat(avg);
			} else {
				long avg = l/n;
		    	return toInt(avg);
			}
		}
	}

	private static class MAX extends Item {
		private double d = Double.NEGATIVE_INFINITY;
		private long l = Long.MIN_VALUE;
		@Override
		void add(Object o) {
			if (isFloat) {
				double d2 = getFloat(o);
				if (d2 > d) {
					d = d2;
				}
			} else {
				long i2 = getInt(o);
				if (i2 > l) {
					l = i2;
				}
			}
		}
		@Override
		Object result() {
			if (isFloat) {
				return toFloat(d);
			} else {
				return toInt(l);
			}
		}
	}
	
	private static class MIN extends Item {
		private double d = Double.MAX_VALUE;
		private long l = Long.MAX_VALUE;
		@Override
		void add(Object o) {
			if (isFloat) {
				double d2 = getFloat(o);
				if (d2 < d) {
					d = d2;
				}
			} else {
				long i2 = getInt(o);
				if (i2 < l) {
					l = i2;
				}
			}
		}
		@Override
		Object result() {
			if (isFloat) {
				return toFloat(d);
			} else {
				return toInt(l);
			}
		}
	}
	
	private static class SUM extends Item {
		private double d;
		private long l;
		@Override
		void add(Object o) {
			if (isFloat) {
				d += getFloat(o);
			} else {
				l += getInt(o);
			}
		}
		@Override
		Object result() {
			if (isFloat) {
				return d;
			} else {
				return l;
			}
		}
	}
	
	private static class COUNT extends Item {
		private long n = 0;
		@Override
		void add(Object o) {
			n++;
		}
		@Override
		Object result() {
			return n;
		}
	}
	
	private static class FIELD extends Item {
		private Object ret = null;
		@Override
		void add(Object o) {
			ret = getValue(o);
		}
		@Override
		Object result() {
			return ret;
		}
	}
	
	/**
	 * 
	 * @param data For example: "avg(salary), sum(salary)".  min, max, avg, sum, count
	 * @param candCls
	 * @param candClsDef 
	 */
	QueryResultProcessor(String data, Class<?> candCls, ZooClassDef candClsDef, 
			Class<?> resultClass) {
		data = data.trim();
		while (data.length() > 0) {
			Item item;
			if (data.startsWith("avg") || data.startsWith("AVG")) {
				item = new AVG();
				data = data.substring(3);
			} else if (data.startsWith("max") || data.startsWith("MAX")) {
				item = new MAX();
				data = data.substring(3);
			} else if (data.startsWith("min") || data.startsWith("MIN")) {
				item = new MIN();
				data = data.substring(3);
			} else if (data.startsWith("sum") || data.startsWith("SUM")) {
				item = new SUM();
				data = data.substring(3);
			} else if (data.startsWith("count") || data.startsWith("COUNT")) {
				item = new COUNT();
				data = data.substring(5);
			} else {
				item = new FIELD(); //simple field
				isProjection = true;
//				throw new JDOUserException("Query result type not recognised: " + data);
			}

			String fieldName;
			if (item instanceof FIELD) {
				//field only
				int i = data.indexOf(',');
				if (i >= 0) {
					fieldName = data.substring(0, i).trim();
					data = data.substring(i).trim();
				} else {
					fieldName = data.trim();
					data = "";
				}
			} else {
				data = data.trim();
				if (data.charAt(0)!='(') {// {startsWith("(")) {
					throw DBLogger.newUser(
							"Query result type corrupted, '(' expected at pos 0: " + data);
				}
				data = data.substring(1);
				
				int i = data.indexOf(')');
				if (i < 0) {
					throw DBLogger.newUser("Query result type corrupted, ')' not found: " + data);
				}
				fieldName = data.substring(0, i).trim();
				data = data.substring(i).trim();

				//remove ')'
				data = data.substring(1).trim();
			}
			
			items.add(item);
			ZooFieldDef def = candClsDef.getAllFieldsAsMap().get(fieldName);
			if (def == null) {
				throw DBLogger.newUser("Invalid fieldname in result definition: " + fieldName);
			}
			item.setField(def, resultClass);//getField(candCls, candClsDef, fieldName));

			if (!data.isEmpty() && data.charAt(0) == ',') {
				data = data.substring(1).trim();
				if (data.isEmpty()) {
					throw DBLogger.newUser("Trailing comma in result definition not allowed.");
				}
			}
		}		
		
		//some verification
		for (Item i: items) {
			if (!(i instanceof FIELD) && isProjection) {
				throw DBLogger.newUser("Mixing of prejection and aggregation is not allowed.");
			}
		}
	}
	
	ArrayList<Object> processResultProjection(Iterator<Object> in, boolean unique) {
		//projections
		ArrayList<Object> r = new ArrayList<Object>();
		if (items.size() == 1) {
			Item it = items.get(0);
			while (in.hasNext()) {
				it.add(in.next());
				r.add( it.result() );
			}
		} else {
			while (in.hasNext()) {
				Object o = in.next();
				Object[] oa = new Object[items.size()];
				r.add(oa);
				int i = 0;
				for (Item it: items) {
					it.add(o);
					oa[i++] = it.result();
				}
			}
		}
		
		if (unique && r.size() > 1) {
			throw DBLogger.newUser("Non-unique result encountered.");
		}
		return r;
	} 

	Object processResultAggregation(Iterator<Object> in) {
		//aggregations
		while (in.hasNext()) {
			Object o = in.next();
			//TODO do this only if UNIQUE is set??? --> check jdo-spec.
			for (Item i: items) {
				i.add(o);
			}
		}
		//prepare returning results
		if (items.size() == 1) {
			return items.get(0).result();
		} else {
			Object[] oa = new Object[items.size()]; 
			for (int i = 0; i < items.size(); i++) {
				oa[i] = items.get(i).result(); 
			}
			return oa;
		}
	}

	boolean isProjection() {
		return isProjection;
	}
}
