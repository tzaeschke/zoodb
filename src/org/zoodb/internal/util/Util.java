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
package org.zoodb.internal.util;

import org.zoodb.api.impl.ZooPCImpl;

public class Util {

	public static final String oidToString(Object oid) {
		Long l = (Long)oid;
		String s = (Long.rotateRight(l, 48) & 0xFFFFL) + ".";
		s += (Long.rotateRight(l, 32) & 0xFFFFL) + ".";
		s += (Long.rotateRight(l, 16) & 0xFFFFL) + ".";
		s += (l & 0xFFFFL);
		return s;
	}

	public static final long stringToOid(String str) {
		int p1 = str.indexOf('.');
		int p2 = str.indexOf(p1, '.');
		int p3 = str.indexOf(p2, '.');
		long l1 = Long.parseLong( str.substring(0, p1) );
		long l2 = Long.parseLong( str.substring(p1+1, p2) );
		long l3 = Long.parseLong( str.substring(p2+1, p3) );
		long l4 = Long.parseLong( str.substring(p3+1) );
		
		long oid = l1 << 48 + l2 << 32 + l3 << 16 + l4;
		return oid;
	}

	public static String getOidAsString(Object obj) {
		if (!(obj instanceof ZooPCImpl)) {
			return "-1.-1.-1.-1";
		}
		return oidToString( ((ZooPCImpl)obj).jdoZooGetOid() );
	}
}
