package org.zoodb.jdo.internal.util;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

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
		if (!(obj instanceof PersistenceCapableImpl)) {
			return "-1.-1.-1.-1";
		}
		return oidToString( ((PersistenceCapableImpl)obj).jdoZooGetOid() );
	}
}
