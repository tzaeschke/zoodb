/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.JDOUserException;

/**
 * Processes query results.
 * 
 * See Jdo 2.2 spec 14.6.9.
 * 
 * @author Tilmann Zäschke
 *
 */
class QueryResultProcessor {

	private final List<Item> items = new LinkedList<Item>();
	
	private static abstract class Item {
		private Field field;
		void setField(Field field) {
			this.field = field;
			
		}
		
	}
	
	private static class AVG extends Item {
		
	}

	private static class MIN extends Item {
		
	}
	
	private static class SUM extends Item {
		
	}
	
	private static class COUNT extends Item {
		
	}
	
	/**
	 * 
	 * @param data For example: "avg(salary), sum(salary)".  min, max, avg, sum, count
	 * @param candCls
	 */
	QueryResultProcessor(String data, Class<?> candCls) {
		while (data.length() > 0) {
			Item item;
			if (data.startsWith("avg") || data.startsWith("AVG")) {
				item = new AVG();
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
				throw new JDOUserException("Query result type not recognised: " + data);
			}
			
			data = data.trim();
			if (data.charAt(0)!='(') {// {startsWith("(")) {
				throw new JDOUserException("Query result type corrupted, '(' expected at pos 0: " + data);
			}
			data = data.substring(1);
			
			int i = data.indexOf(')');
			if (i < 0) {
				throw new JDOUserException("Query result type corrupted, ')' not found: " + data);
			}
			String fieldName = data.substring(0, i).trim();
			data = data.substring(i).trim();

			//remove ')'
			data = data.substring(1).trim();
			
			items.add(item);
			item.setField(getField(candCls, fieldName));

			if (!data.isEmpty() && data.charAt(0)== ',') {
				data = data.substring(1).trim();
			}
		}		
	}
	
	private Field getField(Class<?> cls, String fieldName) {
		Field[] fields = cls.getDeclaredFields();
		for (Field f: fields) {
			if (f.getName().equals(fieldName)) {
				return f;
			}
		}
		cls = cls.getSuperclass();
		if (cls.equals(Object.class)) {
			throw new JDOUserException("Invalid field name: " + fieldName );
		}
		//try super class
		return getField(cls, fieldName);
	}
}
