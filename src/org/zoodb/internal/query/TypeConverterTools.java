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

import org.zoodb.internal.util.DBLogger;

public class TypeConverterTools {

	public static double toDouble(Object o) { 
		if (o instanceof Double) {
			return (double)o; 
		} else if (o instanceof Float) {
			return (float)o; 
		} 
		return toLong(o);
	}
	
	public static long toLong(Object o) { 
		if (o instanceof Long) {
			return (long)o; 
		}
		return toInt(o);
	}

	public static int toInt(Object o) { 
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
}
