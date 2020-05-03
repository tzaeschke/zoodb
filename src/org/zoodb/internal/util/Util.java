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
package org.zoodb.internal.util;

import org.zoodb.api.impl.ZooPC;

public class Util {

	public static String oidToString(Object oid) {
		Long l = (Long)oid;
		String s = (Long.rotateRight(l, 48) & 0xFFFFL) + ".";
		s += (Long.rotateRight(l, 32) & 0xFFFFL) + ".";
		s += (Long.rotateRight(l, 16) & 0xFFFFL) + ".";
		s += (l & 0xFFFFL);
		return s;
	}

	public static long stringToOid(String str) {
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
		if (!(obj instanceof ZooPC)) {
			return "-1.-1.-1.-1";
		}
		return oidToString( ((ZooPC)obj).jdoZooGetOid() );
	}
}
