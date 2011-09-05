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
