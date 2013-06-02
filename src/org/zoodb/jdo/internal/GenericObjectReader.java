package org.zoodb.jdo.internal;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import org.zoodb.jdo.internal.server.index.BitTools;

public class GenericObjectReader implements SerialInput {

	//private static final int S_BOOL = 1;
	private static final int S_BYTE = 1;
	private static final int S_CHAR = 2;
	private static final int S_DOUBLE = 8;
	private static final int S_FLOAT = 4;
	private static final int S_INT = 4;
	private static final int S_LONG = 8;
	private static final int S_SHORT = 2;
	
	private final ByteBuffer buf;
	
	//The header is only written in auto-paging mode
	private long pageHeader = -1;
	
	//TODO remove this
	private final int MAX_POS = Integer.MAX_VALUE;

	public GenericObjectReader(ByteBuffer ba) {
		buf = ba;
		pageHeader = buf.getLong();
	}

//	@Override
//	public void seekPosAP(long pageAndOffs) {
//		int page = BitTools.getPage(pageAndOffs);
//		int offs = BitTools.getOffs(pageAndOffs);
//		seekPage(page, offs);
//	}
//
//	@Override
//	public void seekPage(int pageId, int pageOffset) {
//		throw new UnsupportedOperationException();
//	}

	@Override
	public String readString() {
		checkPosRead(4);
		int len = buf.getInt();

		//Align for 2-byte writing
		int p = buf.position();
		if ((p & 0x00000001) == 1) {
			buf.position(p+1);
		}

		char[] array = new char[len];
		CharBuffer cb = buf.asCharBuffer();
		int l = array.length;
		int posA = 0; //position in array
		while (l > 0) {
			checkPosRead(2);
			int getLen = MAX_POS - buf.position();
			getLen = getLen >> 1;
			if (getLen > l) {
				getLen = l;
			}
			cb = buf.asCharBuffer(); //create a buffer at the correct position
			cb.get(array, posA, getLen);
			buf.position(buf.position()+getLen*2);
			posA += getLen;
			l -= getLen;
		}
		return String.valueOf(array);
		//return String.copyValueOf(array);
		//return new String(array);
	}


	@Override
	public boolean readBoolean() {
		return readByte() != 0;
	}

	@Override
	public byte readByte() {
		checkPosRead(S_BYTE);
		return buf.get();
	}

	@Override
	public char readChar() {
		if (!checkPos(S_CHAR)) {
			return readByteBuffer(S_CHAR).getChar();
		}
		return buf.getChar();
	}

	@Override
	public double readDouble() {
		if (!checkPos(S_DOUBLE)) {
			return Double.longBitsToDouble(readLong());
		}
		return buf.getDouble();
	}

	@Override
	public float readFloat() {
		if (!checkPos(S_FLOAT)) {
			return Float.intBitsToFloat(readInt());
		}
		return buf.getFloat();
	}

	@Override
	public void readFully(byte[] array) {
		int l = array.length;
		int posA = 0; //position in array
		while (l > 0) {
			checkPosRead(1);
			int getLen = MAX_POS - buf.position();
			if (getLen > l) {
				getLen = l;
			}
			buf.get(array, posA, getLen);
			posA += getLen;
			l -= getLen;
		}
	}

	@Override
	public int readInt() {
		if (!checkPos(S_INT)) {
			return readByteBuffer(S_INT).getInt();
		}
		return buf.getInt();
	}

	@Override
	public long readLong() {
		if (!checkPos(S_LONG)) {
			return readByteBuffer(S_LONG).getLong();
		}
		//	checkPosRead(S_LONG);
		return buf.getLong();
	}

	@Override
	public short readShort() {
		if (!checkPos(S_SHORT)) {
			return readByteBuffer(S_SHORT).getShort();
		}
		return buf.getShort();
	}

	private ByteBuffer readByteBuffer(int len) {
		byte[] ba = new byte[len];
		readFully(ba);
		return ByteBuffer.wrap(ba);
	}

	private boolean checkPos(int delta) {
		//TODO remove autopaging, the indices use anyway the noCheckMethods!!
		//TODO -> otherwise, make it final, as it should be known when a view is constructed.
		return true;
	}

	private void checkPosRead(int delta) {
		//TODO remove?
	}

	@Override
	public void skipRead(int nBytes) {
		int l = nBytes;
		while (l > 0) {
			checkPosRead(1);
			int bPos = buf.position();
			int putLen = MAX_POS - bPos;
			if (putLen > l) {
				putLen = l;
			}
			buf.position(bPos + putLen);
			l -= putLen;
		}
	}

    @Override
    public long getHeaderClassOID() {
    	return pageHeader;
    }

}
