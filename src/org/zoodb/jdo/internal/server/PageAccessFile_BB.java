/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal.server;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;

public class PageAccessFile_BB implements SerialInput, SerialOutput, PageAccessFile {

	//private static final int S_BOOL = 1;
	private static final int S_BYTE = 1;
	private static final int S_CHAR = 2;
	private static final int S_DOUBLE = 8;
	private static final int S_FLOAT = 4;
	private static final int S_INT = 4;
	private static final int S_LONG = 8;
	private static final int S_SHORT = 2;
	
	private final ByteBuffer buf;
	private int currentPage = -1;
	
	private final FreeSpaceManager fsm;
	//indicate whether to automatically allocate and move to next page when page end is reached.
	private boolean isAutoPaging = false;
	private boolean isWriting = true;
	//The header is only written in auto-paging mode
	private long pageHeader = -1;
	
	private final int MAX_POS;
	
	private final PageAccessFile_BBRoot root;
	private PagedObjectAccess overflowCallback = null;
	private final IntBuffer intBuffer;
	private final int[] intArray;

	/**
	 * Opens a new file.
	 * @param dbPath
	 * @param options
	 * @param pageSize
	 * @param fsm
	 */
	public PageAccessFile_BB(String dbPath, String options, int pageSize, FreeSpaceManager fsm) {
		MAX_POS = pageSize - 4;
		this.fsm = fsm;
		root = new PageAccessFile_BBRoot(dbPath, options, pageSize);
		try {
			FileChannel fc = root.getFileChannel();
    		isWriting = (fc.size() == 0);
    		buf = ByteBuffer.allocateDirect(pageSize);
    		currentPage = 0;
    
    		//fill buffer
    		buf.clear();
    		int n = fc.read(buf); 
    		if (n != pageSize && fc.size() != 0) {
    			throw new JDOFatalDataStoreException("Bytes read: " + n);
    		}
		} catch (IOException e) {
		    throw new JDOFatalDataStoreException("Error opening database: " + dbPath, e);
		}
		buf.limit(pageSize);
		buf.rewind();
		intBuffer = buf.asIntBuffer();
		intArray = new int[intBuffer.capacity()];
		root.addView(this);
	}

	/**
	 * Use for creating an additional view on a given file.
	 * @param fc
	 * @param pageSize
	 * @param fsm
	 */
	private PageAccessFile_BB(PageAccessFile_BB original) {
		this.root = original.root; 
		this.MAX_POS = original.MAX_POS;
		this.fsm = original.fsm;
		
		isWriting = false;
		buf = ByteBuffer.allocateDirect(root.getPageSize());
		currentPage = 0;
		intBuffer = buf.asIntBuffer();
		intArray = new int[intBuffer.capacity()];
		root.addView(this);
	}

	@Override
	public PageAccessFile split() {
		return new PageAccessFile_BB(this);
	}
	
	@Override
	public void seekPageForRead(int pageId, boolean autoPaging) {
	    seekPage(pageId, 0, autoPaging);
	}
	
	
	@Override
	public void seekPageForWrite(int pageId) {
		isAutoPaging = false;
		writeData();
		isWriting = true;
		currentPage = pageId;
		buf.clear();
	}
	
	@Override
	public void seekPos(long pageAndOffs) {
		int page = BitTools.getPage(pageAndOffs);
		int offs = BitTools.getOffs(pageAndOffs);
		seekPage(page, offs, true);
	}

	@Override
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;

		if (isWriting) {
			writeData();
			isWriting = false;
			throw new RuntimeException();  //TODO writing & seek??? 
		}
		if (pageId != currentPage) {
			currentPage = pageId;
			buf.clear();
			root.readPage(buf, pageId);
		}

		if (isAutoPaging) {
			buf.clear();
			pageHeader = buf.getLong();
			if (pageOffset==0) {
				pageOffset = 8; //TODO this is dirty...
			}
		}
		buf.position(pageOffset);
	}
	
	@Override
	public int allocateAndSeek(int prevPage) {
		isAutoPaging = false;
		return allocateAndSeekPage(prevPage);
	}
	
	@Override
	public int allocateAndSeek(int prevPage, long header) {
		pageHeader = header;
		isAutoPaging = true;
		int pageId = allocateAndSeekPage(prevPage);
		//auto-paging is true
		buf.putLong(pageHeader);
		return pageId;
	}
	
	private int allocateAndSeekPage(int prevPage) {
		int pageId = fsm.getNextPage(prevPage);
		try {
			writeData();
	        isWriting = true;
			currentPage = pageId;
			buf.clear();
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
		return pageId;
	}
	
	@Override
	public void releasePage(int pageId) {
		fsm.reportFreePage(pageId);
	}
	
	@Override
	public void close() {
		flush();
		root.close();
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	@Override
	public void flush() {
		//flush associated splits.
		for (PageAccessFile_BB paf: root.getViews()) {
			//paf.flush(); //This made SerializerTest.largeObject 10 slower ?!!?!?!?
			paf.writeData();
			//To avoid unnecessary writing during the next flush()
			paf.isWriting = false;
		}
		root.flush();
	}
	
	private void writeData() {
		if (isWriting) {
			buf.flip();
			root.write(buf, currentPage);
		}
	}
	
	
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
	public void noCheckRead(long[] array) {
		LongBuffer lb = buf.asLongBuffer();
		lb.get(array);
	    buf.position(buf.position() + S_LONG * array.length);
	}
	
	@Override
	public void noCheckRead(int[] array) {
		IntBuffer lb = buf.asIntBuffer();
		lb.get(array);
	    buf.position(buf.position() + S_INT * array.length);
	}
	
	@Override
	public void noCheckRead(byte[] array) {
		buf.get(array);
	}
	
	@Override
	public void noCheckReadAsInt(long[] array, int nElements) {
		int pos = buf.position();
		if ((pos >> 2) << 2 == pos) {
			intBuffer.position(pos >> 2);
		} else {
			intBuffer.position((pos >> 2)+1);
		}
		intBuffer.get(intArray, 0, nElements);
		for (int i = 0; i < nElements; i++) {
			array[i] = intArray[i];
		}
	    buf.position(intBuffer.position() * S_INT);

	  //Alternative implementation (faster according to PerfTest but slower when running JUnit suite
//		for (int i = 0; i < nElements; i++) {
//			array[i] = buf.getInt();
//		}
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
//		checkPosRead(S_LONG);
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
	
    @Override
    public long readLongAtOffset(int offset) {
        return buf.getLong(0);
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

	/**
	 * The no-check methods are thought to be faster, because they don't need range checking.
	 * Furthermore, they ensure that a page can be filled to the last byte. without a new page
	 * being allocated.
	 */
	@Override
	public void noCheckWrite(long[] array) {
	    LongBuffer lb = buf.asLongBuffer();
	    lb.put(array);
	    buf.position(buf.position() + S_LONG * array.length);
	}

	@Override
	public void noCheckWrite(int[] array) {
	    IntBuffer lb = buf.asIntBuffer();
	    lb.put(array);
	    buf.position(buf.position() + S_INT * array.length);
	}

	@Override
	public void noCheckWrite(byte[] array) {
	    buf.put(array);
	}
	
	@Override
	public void noCheckWriteAsInt(long[] array, int nElements) {
		int pos = buf.position();
		if ((pos >> 2) << 2 == pos) {
			intBuffer.position(pos >> 2);
		} else {
			intBuffer.position((pos >> 2)+1);
		}
		for (int i = 0; i < nElements; i++) {
			intArray[i] = (int) array[i];
		}
	    intBuffer.put(intArray, 0, nElements);
	    buf.position(intBuffer.position() * S_INT);

//Alternative implementation (faster according to PerfTest but slower when running JUnit suite
//		for (int i = 0; i < nElements; i++) {
//			buf.putInt( (int) array[i] );
//		}
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
		if (isAutoPaging) {
			return (buf.position() + delta - MAX_POS) <= 0;
		}
		return true;
	}

	private void checkPosWrite(int delta) {
		if (isAutoPaging && buf.position() + delta > MAX_POS) {
			int pageId = fsm.getNextPage(0);
			buf.putInt(pageId);

			//write page
			writeData();
			currentPage = pageId;
			buf.clear();
			
			buf.putLong(pageHeader);
			if (overflowCallback != null) {
				overflowCallback.notifyOverflowWrite(currentPage);
			}
		}
	}

	private void checkPosRead(int delta) {
		final ByteBuffer buf = this.buf;
		if (isAutoPaging && buf.position() + delta > MAX_POS) {
			final int pageId = buf.getInt();
			currentPage = pageId;
			buf.clear();
			root.readPage(buf, pageId);
			buf.rewind();
			//read header
			pageHeader = buf.getLong();
		}
 	}

    @Override
    public int getOffset() {
        return buf.position();
    }

    @Override
    public int getPage() {
        return currentPage;
    }
	
	@Override
	public int statsGetWriteCount() {
		return root.statsGetWriteCount();
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
	public int getPageSize() {
		return (int) root.getPageSize();
	}

	/**
	 * Set a call-back for this view. Every view has its own call-backs.
	 */
    @Override
    public void setOverflowCallback(PagedObjectAccess overflowCallback) {
        this.overflowCallback = overflowCallback;
    }
}
