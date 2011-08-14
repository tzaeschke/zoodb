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

}
