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
import java.util.Comparator;
import java.util.List;

import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Pair;

public class QueryComparator<T> implements Comparator<T> {

	private final List<Pair<ZooFieldDef, Boolean>> ordering;
	
	public QueryComparator(List<Pair<ZooFieldDef, Boolean>> ordering) {
		this.ordering = ordering;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compare(Object o1, Object o2) {
		for (Pair<ZooFieldDef, Boolean> p: ordering) {
			ZooFieldDef fd = p.getA();
			Field f = fd.getJavaField();

			Object o1Val;
			try {
				o1Val = f.get(o1);
			} catch (IllegalArgumentException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + fd.getName() + 
						" class=\"" + o1.getClass().getName() + "\"," + 
						" declaring class=\"" + f.getDeclaringClass().getName()+ "\"", e);
			} catch (IllegalAccessException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + fd.getName(), e);
			}

			Object o2Val;
			try {
				o2Val = f.get(o2);
			} catch (IllegalArgumentException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + fd.getName() + 
						" class=\"" + o2.getClass().getName() + "\"," + 
						" declaring class=\"" + f.getDeclaringClass().getName()+ "\"", e);
			} catch (IllegalAccessException e) {
				throw DBLogger.newFatalInternal("Cannot access field: " + fd.getName(), e);
			}
			
			if (o1Val == null) {
				if (o2Val == null) {
					continue;
				}
				//ordering of null-value fields is not specified (JDO 3.0 14.6.6: Ordering Statement)
				//We specify:  (null <= 'x' ==true)
				if (o2Val != null) {
					return ret(-1, p);
				}
			} else if (o2Val != null && o1Val != null) {
				if (o2Val.equals(o1Val)) {
					continue;
				}
				if (o2Val instanceof Comparable) {
					Comparable<Object> qComp = (Comparable<Object>) o2Val;
					int res = -qComp.compareTo(o1Val);  //-1:<   0:==  1:> 
					if (res == 0) {
						continue;
					} else {
						return ret(res, p);
					}
				}
			} else {
				//here: o2Val == null && o1Val != null
				//Ordering of null-value fields is not specified (JDO 3.0 14.6.6: Ordering Statement)
				//We specify:  (null <= 'x' ==true)
				return ret(1, p);
			}
		}
		return 0;
	}
	
	private int ret(int result, Pair<ZooFieldDef, Boolean> p) {
		return p.getB() ? result : -result;
	}

}
