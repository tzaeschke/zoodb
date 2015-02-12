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

import org.zoodb.internal.ZooFieldDef;

/**
 * Query functions.
 * 
 * @author ztilmann
 *
 */
public abstract class QueryFunction {

	protected QueryFunction inner;
	
	abstract Object evaluate(Object instance);
	
	public void setInner(QueryFunction fn) {
		if (inner != null) {
			throw new IllegalStateException();
		}
		inner = fn;
	}
	
	
	public static class Path extends QueryFunction {
		
		private final String fieldName;
		private final ZooFieldDef zField;
		private final Field field;
		
		public Path(String fieldName, ZooFieldDef zField, Field field) {
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

}
