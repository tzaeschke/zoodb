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
					if (res != 0) {
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
