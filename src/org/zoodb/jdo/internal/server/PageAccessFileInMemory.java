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

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.jdo.api.impl.DataStoreManagerInMemory;
import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;

public class PageAccessFileInMemory implements SerialInput, SerialOutput, PageAccessFile {

	private static final int S_BYTE = 1;
	private static final int S_CHAR = 2;
	private static final int S_DOUBLE = 8;
	private static final int S_FLOAT = 4;
	private static final int S_INT = 4;
	private static final int S_LONG = 8;
	private static final int S_SHORT = 2;

	private ByteBuffer buf;
	private int currentPage = -1;
	private int statNWrite = 0;
	private boolean isAutoPaging = false;
	//The header is only written in auto-paging mode
	private long pageHeader = -1;
	
	// use bucket version of array List
	private final ArrayList<ByteBuffer> buffers;
	
	private final int PAGE_SIZE;
	private final int MAX_POS;
	
	private final List<PageAccessFileInMemory> splits = new LinkedList<PageAccessFileInMemory>();
	private PagedObjectAccess overflowCallback = null;

	//TODO try introducing this down-counter. it may be faster than checking _buf.position() all
	//the time. Of course this requires that we update the PagedObjectWriter such that autopaging
	//is done here. In fact auto-paging is not even implemented here.
	private int downCnt;
	private boolean isWriting = false;
	private final FreeSpaceManager fsm;
	private IntBuffer intBuffer;
	private final int[] intArray;

	
	/**
	 * Constructor for use by DataStoreManager.
	 * @param dbPath
	 * @param options
	 */
	public PageAccessFileInMemory(String dbPath, String options, int pageSize, 
			FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		MAX_POS = PAGE_SIZE - 4;
		downCnt = MAX_POS + 4;
		this.fsm = fsm;
		// We keep the arguments to allow transparent dependency injection.
		buffers = DataStoreManagerInMemory.getInternalData(dbPath);
		if (!buffers.isEmpty()) {
			buf = buffers.get(0);
			buf.rewind();
			isWriting = false;
		}
		currentPage = 0;
		intArray = new int[PAGE_SIZE >> 2];
	}
	
	/**
	 * Constructor for direct use in test harnesses, e.g. for index testing.
	 */
	public PageAccessFileInMemory(int pageSize, FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		MAX_POS = PAGE_SIZE - 4;
		downCnt = MAX_POS + 4;
		this.fsm = fsm;
		buffers = new ArrayList<ByteBuffer>();
		buffers.add( ByteBuffer.allocateDirect(PAGE_SIZE) );
		buf = buffers.get(0);
		currentPage = 0;
		intArray = new int[PAGE_SIZE >> 2];
	}

	private PageAccessFileInMemory(ArrayList<ByteBuffer> buffers, int pageSize, 
			FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		MAX_POS = PAGE_SIZE - 4;
		downCnt = MAX_POS + 4;
		this.fsm = fsm;
		this.buffers = buffers;
		buf = this.buffers.get(0);
		currentPage = 0;
		intArray = new int[PAGE_SIZE >> 2];
	}

	@Override
	public PageAccessFile split() {
		PageAccessFileInMemory split = new PageAccessFileInMemory(buffers, PAGE_SIZE, fsm);
		splits.add(split);
		return split;
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

		while (pageId >= buffers.size()) {
			ByteBuffer buf = ByteBuffer.allocateDirect(PAGE_SIZE);
			buffers.add( buf );
			//This does not allow simulating database recovery. Due to a crashed database there
			//may be more buffers in the list than are actually required. We would need to count
			//used pages instead of using _buffers.size().
		}
		buf = buffers.get(pageId);
		buf.rewind();
		//downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
		downCnt = MAX_POS + 4;
	}
	
	@Override
	public void seekPos(long pageAndOffs) {
		int page = (int)(pageAndOffs >> 32);
		int offs = (int)(pageAndOffs & 0x000000007FFFFFFF);
		seekPage(page, offs, true);
	}

	@Override
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;

		if (isWriting) {
            writeData();
            isWriting = false;
		}
		if (pageId != currentPage) {
			currentPage = pageId;
			buf = buffers.get(pageId);
			//set limit to PAGE_SIZE, in case we were reading the last current page, or even
			//a completely new page.
			buf.limit(PAGE_SIZE);
		}

		if (isAutoPaging) {
			buf.clear();
			pageHeader = buf.getLong();
			if (pageOffset==0) {
				pageOffset = 8; //TODO this is dirty...
			}
		}
		buf.position(pageOffset);
		downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
		downCnt += pageOffset;
	}
	
	@Override
	public int allocateAndSeek(int prevPage) {
		isAutoPaging = false;
		int pageId = allocateAndSeekPage(prevPage);
		//auto-paging is true
		downCnt = MAX_POS + 4;
		return pageId;
	}
	
	@Override
	public int allocateAndSeek(int prevPage, long header) {
		pageHeader = header;
		isAutoPaging = true;
		int pageId = allocateAndSeekPage(prevPage);
		//auto-paging is true
		downCnt = MAX_POS;
		buf.putLong(pageHeader);
		return pageId;
	}
	
	private int allocateAndSeekPage(int prevPage) {
		writeData();
		int pageId = allocatePage(prevPage);
		isWriting = true;
		currentPage = pageId;
		buf = buffers.get(pageId);

		buf.rewind();
		return pageId;
	}
	
	private int allocatePage(int prevPage) {
		statNWrite++;
		
		int pageId = fsm.getNextPage(prevPage);
		if (pageId >= buffers.size()) {
			ByteBuffer buf = ByteBuffer.allocateDirect(PAGE_SIZE);
			buffers.add( buf );
			//This does not allow simulating database recovery. Due to a crashed database there
			//may be more buffers in the list than are actually required. We would need to count
			//used pages instead of using _buffers.size().
			return buffers.size()-1;
		}
		
		return pageId;
	}
	
	@Override
	public void releasePage(int pageId) {
		fsm.reportFreePage(pageId);
	}

	public void close() {
		flush();
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	public void flush() {
		for (PageAccessFileInMemory paf: splits) {
			paf.flush();
			paf.isWriting = false;
		}
		isWriting = false;
		writeData();
	}
	
	private void writeData() {
		//TODO this flag needs only to be set after seek. I think. Remove updates in write methods.
	}
	
	public String readString() {
		checkPosRead(4);
		int len = buf.getInt();
		byte[] ba = new byte[len];
		readFully(ba);
		return new String(ba);
	}

	public void writeString(String string) {
		checkPosWrite(4);
        byte[] ba = string.getBytes();
		buf.putInt(ba.length);
		write(ba);
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
//		int pos = buf.position();
//		if ((pos >> 2) << 2 == pos) {
//			intBuffer.position(pos >> 2);
//		} else {
//			intBuffer.position((pos >> 2)+1);
//		}
//		intBuffer.get(intArray, 0, nElements);
//		for (int i = 0; i < nElements; i++) {
//			array[i] = intArray[i];
//		}
//	    buf.position(intBuffer.position() * S_INT);

	  //Alternative implementation (faster according to PerfTest but slower when running JUnit suite
		for (int i = 0; i < nElements; i++) {
			array[i] = buf.getInt();
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
//		int pos = buf.position();
//		if ((pos >> 2) << 2 == pos) {
//			intBuffer.position(pos >> 2);
//		} else {
//			intBuffer.position((pos >> 2)+1);
//		}
//		for (int i = 0; i < nElements; i++) {
//			intArray[i] = (int) array[i];
//		}
//	    intBuffer.put(intArray, 0, nElements);
//	    buf.position(intBuffer.position() * S_INT);

		//Alternative implementation (faster according to PerfTest but slower when running JUnit suite
		for (int i = 0; i < nElements; i++) {
			buf.putInt( (int) array[i] );
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

	@Override
	public int getOffset() {
		return buf.position();
	}
	
	@Override
	public int statsGetWriteCount() {
		int ret = statNWrite;
		for (PageAccessFileInMemory p: splits) {
			ret += p.statNWrite;
		}
		return ret;
	}

    @Override
    public int getPage() {
        return currentPage;
    }

	private boolean checkPos(int delta) {
		//TODO remove autopaging, the indices use anyway the noCheckMethods!!
		if (isAutoPaging) {
			return (buf.position() + delta - MAX_POS) <= 0;
		}
		return true;
	}

	private void checkPosWrite(int delta) {
		if (isAutoPaging && buf.position() + delta > MAX_POS) {
			int pageId = allocatePage(0);
			buf.putInt(pageId);

			//write page
			writeData();
			currentPage = pageId;
			buf = buffers.get(pageId);
			buf.clear();
			
			buf.putLong(pageHeader);
			if (overflowCallback != null) {
				overflowCallback.notifyOverflowWrite(currentPage);
			}
		}
	}

	private void checkPosRead(int delta) {
		if (isAutoPaging && buf.position() + delta > MAX_POS) {
			currentPage = buf.getInt();
			buf.clear();
			buf = buffers.get(currentPage);
			buf.rewind();
			//read header
			pageHeader = buf.getLong();
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
		return PAGE_SIZE;
	}

	@Override
	public void setOverflowCallback(PagedObjectAccess overflowCallback) {
		this.overflowCallback = overflowCallback;
	}
}
