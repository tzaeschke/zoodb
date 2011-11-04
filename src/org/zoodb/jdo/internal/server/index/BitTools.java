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
package org.zoodb.jdo.internal.server.index;

public class BitTools {

    public static final long getMinPosInPage(long pos) {
        return pos & 0xFFFFFFFF00000000L;
    }

    public static final long getMaxPosInPage(long pos) {
        return pos | 0x00000000FFFFFFFFL;
    }

    public static final int getPage(long pos) {
        return (int) (pos >> 32);
    }

    public static final int getOffs(long pos) {
        return (int)(pos & 0x00000000FFFFFFFFL);
    }

    public static final long getPos(int pageId, int offs) {
        return (((long)pageId) << 32L) + offs;
    }

	public static long toSortableLong(double value) {
		//TODO is this correct? I.e. (f1 > f2) <==> (l1 > l2) ?
		return Double.doubleToRawLongBits(value);
	}

	public static long toSortableLong(float value) {
		//TODO is this correct? I.e. (f1 > f2) <==> (l1 > l2) ?
		return Float.floatToRawIntBits(value);
	}

	public static double toDouble(long value) {
		//TODO is this correct?
		return Double.longBitsToDouble(value);
	}

	public static float toFloat(long value) {
		//TODO is this correct?
		return Float.intBitsToFloat((int) value);
	}

	public static long toSortableLong(String s) {
    	// store magic number: 6 chars + (hash >> 16)
		long n = 0;
    	int i = 0;
    	for ( ; i < 6 && i < s.length(); i++ ) {
    		n |= (byte) s.charAt(i);
    		n = n << 8;
    	}
    	//Fill with empty spaces if string is too short
    	for ( ; i < 6; i++) {
    		n = n << 8;
    	}
    	n = n << 8;

    	//add hashcode
    	n |= (0xFFFF & s.hashCode());
		return n;
	}

	public static long toSortableLongMaxHash(String s) {
		return toSortableLong(s) | 0xFFFFL;
	}
	
	public static long toSortableLongMinHash(String s) {
		return toSortableLong(s) & 0xFFFFFFFFFFFF0000L;
	}

}
