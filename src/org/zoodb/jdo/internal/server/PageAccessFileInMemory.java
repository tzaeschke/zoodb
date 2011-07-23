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

	private ByteBuffer _buf;
	private int _currentPage = -1;
	private int statNWrite = 0;
	private boolean isAutoPaging = false;
	
	// use bucket version of array List
	private final ArrayList<ByteBuffer> _buffers;
	
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
		_buffers = DataStoreManagerInMemory.getInternalData(dbPath);
		if (!_buffers.isEmpty()) {
			_buf = _buffers.get(0);
			_buf.rewind();
			isWriting = false;
		}
		_currentPage = 0;
	}
	
	/**
	 * Constructor for direct use in test harnesses, e.g. for index testing.
	 */
	public PageAccessFileInMemory(int pageSize, FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		MAX_POS = PAGE_SIZE - 4;
		downCnt = MAX_POS + 4;
		this.fsm = fsm;
		_buffers = new ArrayList<ByteBuffer>();
		_buffers.add( ByteBuffer.allocateDirect(PAGE_SIZE) );
		_buf = _buffers.get(0);
		_currentPage = 0;
	}

	private PageAccessFileInMemory(ArrayList<ByteBuffer> buffers, int pageSize, 
			FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		MAX_POS = PAGE_SIZE - 4;
		downCnt = MAX_POS + 4;
		this.fsm = fsm;
		_buffers = buffers;
		_buf = _buffers.get(0);
		_currentPage = 0;
	}

	@Override
	public PageAccessFile split() {
		PageAccessFileInMemory split = new PageAccessFileInMemory(_buffers, PAGE_SIZE, fsm);
		splits.add(split);
		return split;
	}
	
	@Override
	public void seekPageForRead(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;

		writeData();
		isWriting = false;
		_currentPage = pageId;

		_buf = _buffers.get(pageId);
		_buf.rewind();
		downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
	}
	
	@Override
	public void seekPageForWrite(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;

		writeData();
		isWriting = true;
		_currentPage = pageId;

		_buf = _buffers.get(pageId);
		_buf.rewind();
		downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
	}
	
	@Override
	public void seekPos(long pageAndOffs, boolean autoPaging) {
		int page = (int)(pageAndOffs >> 32);
		int offs = (int)(pageAndOffs & 0x000000007FFFFFFF);
		seekPage(page, offs, autoPaging);
	}

	@Override
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;

		if (pageOffset < 0) {
			pageId--;
			pageOffset += PAGE_SIZE;
		}
		if (pageId != _currentPage) {
			writeData();
			_currentPage = pageId;
			_buf = _buffers.get(pageId);
			_buf.rewind();
			//set limit to PAGE_SIZE, in case we were reading the last current page, or even
			//a completely new page.
			_buf.limit(PAGE_SIZE);
		} else {
			_buf.rewind();
		}
		isWriting = false;
		_buf.position(pageOffset);
		downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
		downCnt += pageOffset;
	}
	
	@Override
	public int allocateAndSeek(boolean autoPaging, int prevPage) {
		isAutoPaging = autoPaging;

		writeData();
		int pageId = allocatePage(prevPage);
		isWriting = true;
		_currentPage = pageId;
		_buf = _buffers.get(pageId);

		_buf.rewind();
		downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
		return pageId;
	}
	
	private int allocatePage(int prevPage) {
		statNWrite++;
		
		int pageId = fsm.getNextPage(prevPage);
		if (pageId >= _buffers.size()) {
			ByteBuffer buf = ByteBuffer.allocateDirect(PAGE_SIZE);
			_buffers.add( buf );
			//This does not allow simulating database recovery. Due to a crashed database there
			//may be more buffers in the list than are actually required. We would need to count
			//used pages instead of using _buffers.size().
			return _buffers.size()-1;
		}
		
		return pageId;
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
		}
		writeData();
	}
	
	private void writeData() {
		//TODO this flag needs only to be set after seek. I think. Remove updates in write methods.
	}
	
	public String readString() {
		checkPosRead(4);
		int len = _buf.getInt();
		byte[] ba = new byte[len];
		readFully(ba);
		return new String(ba);
	}

	public void writeString(String string) {
		checkPosWrite(4);
        byte[] ba = string.getBytes();
		_buf.putInt(ba.length);
		write(ba);
	}

	@Override
	public boolean readBoolean() {
        return readByte() != 0;
	}

	@Override
	public byte readByte() {
		checkPosRead(S_BYTE);
		return _buf.get();
	}

	@Override
	public char readChar() {
		if (!checkPos(S_CHAR)) {
			return readByteBuffer(S_CHAR).getChar();
		}
		return _buf.getChar();
	}

	@Override
	public double readDouble() {
		if (!checkPos(S_DOUBLE)) {
			return Double.longBitsToDouble(readLong());
		}
		return _buf.getDouble();
	}

	@Override
	public float readFloat() {
		if (!checkPos(S_FLOAT)) {
			return Float.intBitsToFloat(readInt());
		}
		return _buf.getFloat();
	}

	@Override
	public void readFully(byte[] array) {
        int l = array.length;
        int posA = 0; //position in array
        while (l > 0) {
            checkPosRead(1);
            int getLen = MAX_POS - _buf.position();
            if (getLen > l) {
                getLen = l;
            }
            _buf.get(array, posA, getLen);
            posA += getLen;
            l -= getLen;
        }
	}

	@Override
	public void noCheckRead(long[] array) {
		LongBuffer lb = _buf.asLongBuffer();
		lb.get(array);
	    _buf.position(_buf.position() + S_LONG * array.length);
	}
	
	@Override
	public void noCheckRead(int[] array) {
		IntBuffer lb = _buf.asIntBuffer();
		lb.get(array);
	    _buf.position(_buf.position() + S_INT * array.length);
	}
	
	@Override
	public int readInt() {
		if (!checkPos(S_INT)) {
			return readByteBuffer(S_INT).getInt();
		}
        return _buf.getInt();
	}

	@Override
	public long readLong() {
		if (!checkPos(S_LONG)) {
			return readByteBuffer(S_LONG).getLong();
		}
		return _buf.getLong();
	}

	@Override
	public short readShort() {
		if (!checkPos(S_SHORT)) {
			return readByteBuffer(S_SHORT).getShort();
		}
		return _buf.getShort();
	}

	private ByteBuffer readByteBuffer(int len) {
		byte[] ba = new byte[len];
		readFully(ba);
		return ByteBuffer.wrap(ba);
	}
	
	@Override
	public void write(byte[] array) {
		int l = array.length;
		int posA = 0; //position in array
		while (l > 0) {
		    checkPosWrite(1);
		    int putLen = MAX_POS - _buf.position();
		    if (putLen > l) {
		        putLen = l;
		    }
		    _buf.put(array, posA, putLen);
		    posA += putLen;
		    l -= putLen;
		}
	}

	@Override
	public void noCheckWrite(long[] array) {
	    LongBuffer lb = _buf.asLongBuffer();
	    lb.put(array);
	    _buf.position(_buf.position() + S_LONG * array.length);
	}

	@Override
	public void noCheckWrite(int[] array) {
	    IntBuffer lb = _buf.asIntBuffer();
	    lb.put(array);
	    _buf.position(_buf.position() + S_INT * array.length);
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		writeByte((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		checkPosWrite(S_BYTE);
		_buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		if (!checkPos(S_CHAR)) {
			write(ByteBuffer.allocate(S_CHAR).putChar(char1).array());
			return;
		}
		_buf.putChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		if (!checkPos(S_DOUBLE)) {
			writeLong(Double.doubleToLongBits(double1));
			return;
		}
		_buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		if (!checkPos(S_FLOAT)) {
			writeInt(Float.floatToIntBits(float1));
			return;
		}
		_buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		if (!checkPos(S_INT)) {
			write(ByteBuffer.allocate(S_INT).putInt(int1).array());
			return;
		}
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		if (!checkPos(S_LONG)) {
			write(ByteBuffer.allocate(S_LONG).putLong(long1).array());
			return;
		}
		_buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		if (!checkPos(S_SHORT)) {
			write(ByteBuffer.allocate(S_SHORT).putShort(short1).array());
			return;
		}
		_buf.putShort(short1);
	}

	@Override
	public int getOffset() {
		return _buf.position();
	}

	@Override
	public void assurePos(int currentPage, int currentOffs) {
		if (currentPage != _currentPage || currentOffs != _buf.position()) {
			System.out.println("assurePos! *************************************************");
			seekPage(currentPage, currentOffs, isAutoPaging);
			throw new RuntimeException();
		}
	}
	
	@Override
	public int statsGetWriteCount() {
		return statNWrite;
	}

    @Override
    public int getPage() {
        return _currentPage;
    }

	private boolean checkPos(int delta) {
		//TODO remove autopaging, the indices use anyway the noCheckMethods!!
		if (isAutoPaging) {
			return (_buf.position() + delta - MAX_POS) <= 0;
		}
		return true;
	}

	private void checkPosWrite(int delta) {
		if (isAutoPaging && _buf.position() + delta > MAX_POS) {
			int pageId = allocatePage(0);
			_buf.putInt(pageId);

			//write page
			writeData();
			_currentPage = pageId;
			_buf = _buffers.get(pageId);
			_buf.clear();
			
			if (overflowCallback != null) {
				overflowCallback.notifyOverflow(_currentPage);
			}
		}
	}

	private void checkPosRead(int delta) {
		if (isAutoPaging && _buf.position() + delta > MAX_POS) {
			_currentPage = _buf.getInt();
			_buf.clear();
			_buf = _buffers.get(_currentPage);
			_buf.rewind();
		}
	}

	@Override
	public void skipWrite(int nBytes) {
	    int l = nBytes;
	    while (l > 0) {
	        checkPosWrite(1);
	        int bPos = _buf.position();
	        int putLen = MAX_POS - bPos;
	        if (putLen > l) {
	            putLen = l;
	        }
	        _buf.position(bPos + putLen);
	        l -= putLen;
	    }
	}

	@Override
	public void skipRead(int nBytes) {
        int l = nBytes;
        while (l > 0) {
            checkPosRead(1);
            int bPos = _buf.position();
            int putLen = MAX_POS - bPos;
            if (putLen > l) {
                putLen = l;
            }
            _buf.position(bPos + putLen);
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
