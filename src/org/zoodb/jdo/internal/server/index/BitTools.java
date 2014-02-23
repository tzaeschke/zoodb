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
package org.zoodb.jdo.internal.server.index;

public class BitTools {

	/** Value to recognize 'null'in indices. Using MIN_VALUE so that NULL is the lowest value
	 * when sorted. */ 
	public static final long NULL = Long.MIN_VALUE;
	
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
        return (((long)pageId) << 32) + offs;
    }

    /**
     * WARNING
     * This method turns -0.0 into 0.0. Therefore -0.0 is not smaller than 0.0 when stored in
     * an index.
     * @param value
     * @return long representation.
     */
	public static long toSortableLong(double value) {
		//To create a sortable long, we convert the double to a long using the IEEE-754 standard,
		//which stores floats in the form <sign><exponent-127><mantissa> .
		//This result is properly ordered longs for all positive doubles. Negative values have
		//inverse ordering. For negative doubles, we therefore simply invert them to make them 
		//sortable, however the sign must be inverted again to stay negative.
		if (value == -0.0) {
			value = 0.0;
		}
		if (value < 0.0) {
			long l = Double.doubleToRawLongBits(value);
			l = ~l;
			l |= (1l << 63l);
			return l;
		}
		return Double.doubleToRawLongBits(value);
	}

	public static long toSortableLong(float value) {
		//see toSortableLong(double)
		if (value == -0.0) {
			value = 0.0f;
		}
		if (value < 0.0) {
			int l = Float.floatToRawIntBits(value);
			l = ~l;
			l |= (1l << 31l);
			return l;
		}
		return Float.floatToRawIntBits(value);
	}

	public static double toDouble(long value) {
		if (value < 0.0) {
			long l = value;
			l = ~l;
			l |= (1l << 63l);
			return Double.longBitsToDouble(l);
		}
		return Double.longBitsToDouble(value);
	}

	public static float toFloat(long value) {
		if (value < 0.0) {
			int l = (int) value;
			l = ~l;
			l |= (1l << 31l);
			return Float.intBitsToFloat(l);
		}
		return Float.intBitsToFloat((int) value);
	}

	public static long toSortableLong(String s) {
		if (s == null) {
			return NULL;
		}
		
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
