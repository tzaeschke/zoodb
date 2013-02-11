/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import org.zoodb.jdo.internal.server.ObjectWriter;
import org.zoodb.jdo.internal.server.index.PagedPosIndex;


/**
 * 
 * @author Tilmann Zaschke
 *
 */
class GenericObjectWriter implements ObjectWriter {

	//private static final int S_BOOL = 1;
	private static final int S_BYTE = 1;
	private static final int S_CHAR = 2;
	private static final int S_DOUBLE = 8;
	private static final int S_FLOAT = 4;
	private static final int S_INT = 4;
	private static final int S_LONG = 8;
	private static final int S_SHORT = 2;
	
	private final ByteBuffer buf;
	private final long headerForWrite;
	
	private final int MAX_POS;
	
	public GenericObjectWriter(int nBytes, long clsOid) {
		this.buf = ByteBuffer.allocate(nBytes);
        this.headerForWrite = clsOid;
        this.MAX_POS = nBytes;
	}

	@Override
	public void startObject(long oid) {
		//nothing to do...
	}
	
    @Override
	public void notifyOverflowWrite(int newPage) {
    	throw new UnsupportedOperationException();
    }

	@Override
	public void finishObject() {
		//nothing to do...
	}
	
	/**
	 * This can be necessary when subsequent objects are of a different class.
	 */
	@Override
	public void newPage(PagedPosIndex[] posIndex) {
		writeHeader();
	}

	private void writeHeader() {
		writeLong(headerForWrite);
	}
	
	@Override
	public void writeString(String string) {
        checkPosWrite(4);
        buf.putInt(string.length());

        //Align for 2-byte writing
        int p = buf.position();
        if ((p & 0x00000001) == 1) {
            buf.position(p+1);
        }
        
        CharBuffer cb;
        int l = string.length();
        int posA = 0; //position in array
        while (l > 0) {
            checkPosWrite(2);
            int putLen = MAX_POS - buf.position();
            putLen = putLen >> 1; //TODO loses odd values!
            if (putLen > l) {
                putLen = l;
            }
            //This is crazy!!! Unlike ByteBuffer, CharBuffer requires END as third param!!
            cb = buf.asCharBuffer();
            cb.put(string, posA, posA + putLen);
            buf.position(buf.position() + putLen * 2);

            posA += putLen;
            l -= putLen;
        }
	}

	@Override
	public void write(byte[] array) {
		int l = array.length;
		int posA = 0; //position in array
		while (l > 0) {
		    checkPosWrite(1);
		    int putLen = MAX_POS - buf.position();
		    if (putLen > l) {
		        putLen = l;
		    }
		    buf.put(array, posA, putLen);
		    posA += putLen;
		    l -= putLen;
		}
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		writeByte((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		checkPosWrite(S_BYTE);
		buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		if (!checkPos(S_CHAR)) {
			write(ByteBuffer.allocate(S_CHAR).putChar(char1).array());
			return;
		}
		buf.putChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		if (!checkPos(S_DOUBLE)) {
			writeLong(Double.doubleToLongBits(double1));
			return;
		}
		buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		if (!checkPos(S_FLOAT)) {
			writeInt(Float.floatToIntBits(float1));
			return;
		}
		buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		if (!checkPos(S_INT)) {
			write(ByteBuffer.allocate(S_INT).putInt(int1).array());
			return;
		}
		buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		if (!checkPos(S_LONG)) {
			write(ByteBuffer.allocate(S_LONG).putLong(long1).array());
			return;
		}
		buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		if (!checkPos(S_SHORT)) {
			write(ByteBuffer.allocate(S_SHORT).putShort(short1).array());
			return;
		}
		buf.putShort(short1);
	}
	
	private boolean checkPos(int delta) {
		//TODO remove autopaging, the indices use anyway the noCheckMethods!!
		//TODO -> otherwise, make it final, as it should be known when a view is constructed.
//		if (isAutoPaging) {
//			return (buf.position() + delta - MAX_POS) <= 0;
//		}
		return true;
	}

	private void checkPosWrite(int delta) {
		if (buf.position() + delta > MAX_POS) {
			throw new ArrayIndexOutOfBoundsException((buf.position() + delta) + " > " + MAX_POS);
		}
	}

	@Override
	public void skipWrite(int nBytes) {
	    int l = nBytes;
	    while (l > 0) {
	        checkPosWrite(1);
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
	public void flush() {
		//nothing to do...
	}

	public ByteBuffer toByteArray() {
		return buf;
	}
}
