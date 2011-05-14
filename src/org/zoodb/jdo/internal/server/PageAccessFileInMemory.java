package org.zoodb.jdo.internal.server;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

import org.zoodb.jdo.custom.DataStoreManagerInMemory;
import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;
import org.zoodb.jdo.stuff.BucketArrayList;

public class PageAccessFileInMemory implements SerialInput, SerialOutput, PageAccessFile {

	private ByteBuffer _buf;
	private int _currentPage = -1;
	private boolean _currentPageHasChanged = false;
	private int statNWrite = 0;
	private boolean isAutoPaging = false;
	
	// use bucket version of array List
	private final BucketArrayList<ByteBuffer> _buffers;
	
	private final int PAGE_SIZE;
	private final int MAX_POS;
	
	//TODO try introducing this down-counter. it may be faster than checking _buf.position() all
	//the time. Of course this requires that we update the PagedObjectWriter such that autopaging
	//is done here. In fact auto-paging is not even implemented here.
	private int downCnt;
	
	/**
	 * Constructor for use by DataStoreManager.
	 * @param dbPath
	 * @param options
	 */
	public PageAccessFileInMemory(String dbPath, String options, int pageSize) {
		PAGE_SIZE = pageSize;
		MAX_POS = PAGE_SIZE - 4;
		downCnt = MAX_POS + 4;
		// We keep the arguments to allow transparent dependency injection.
		_buffers = DataStoreManagerInMemory.getInternalData(dbPath);
		if (!_buffers.isEmpty()) {
			_buf = _buffers.get(0);
			_buf.rewind();
		}
		_currentPage = 0;
	}
	
	/**
	 * Constructor for direct use in test harnesses, e.g. for index testing.
	 */
	public PageAccessFileInMemory(int pageSize) {
		PAGE_SIZE = pageSize;
		MAX_POS = PAGE_SIZE - 4;
		downCnt = MAX_POS + 4;
		_buffers = new BucketArrayList<ByteBuffer>();
		_buffers.add( ByteBuffer.allocateDirect(PAGE_SIZE) );
		_buf = _buffers.get(0);
		_currentPage = 0;
	}

	@Override
	public void seekPage(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;

		writeData();
		_currentPage = pageId;

		_buf = _buffers.get(pageId);
		_buf.rewind();
		downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
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
		_buf.position(pageOffset);
		downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
		downCnt += pageOffset;
	}
	
	@Override
	public int allocateAndSeek(boolean autoPaging) {
		isAutoPaging = autoPaging;

		writeData();
		int pageId = allocatePage();
		_currentPage = pageId;

		_buf.rewind();
		downCnt = isAutoPaging ? MAX_POS : MAX_POS + 4;
		return pageId;
	}
	
	private int allocatePage() {
		statNWrite++;
		_buf = ByteBuffer.allocateDirect(PAGE_SIZE);
		_buffers.add( _buf );
		return _buffers.size()-1;
	}

	public void close() {
		flush();
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	public void flush() {
		writeData();
	}
	
	private void writeData() {
		//TODO this flag needs only to be set after seek. I think. Remove updates in write methods.
		if (_currentPageHasChanged) {
			_buf.flip();
			//_fc.write(_buf, _currentPage * DiskAccessOneFile.PAGE_SIZE);
			_currentPageHasChanged = false;
		}
	}
	
	public String readString() {
		int len = _buf.getInt(); //max 127
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char b = (char) _buf.get();
			sb.append(b);
		}
		return sb.toString();
	}

	public void writeString(String string) {
		_buf.putInt(string.length()); //max 127
		for (int i = 0; i < string.length(); i++) {
			_buf.put((byte) string.charAt(i));
		}
	}

	@Override
	public boolean readBoolean() {
		return _buf.get() != 0;
	}

	@Override
	public byte readByte() {
		return _buf.get();
	}

	@Override
	public char readChar() {
		return _buf.getChar();
	}

	@Override
	public double readDouble() {
		return _buf.getDouble();
	}

	@Override
	public float readFloat() {
		return _buf.getFloat();
	}

	@Override
	public void readFully(byte[] array) {
		_buf.get(array);
	}

	@Override
	public int readInt() {
		return _buf.getInt();
	}

	@Override
	public long readLong() {
		return _buf.getLong();
	}

	@Override
	public short readShort() {
		return _buf.getShort();
	}

	@Override
	public void write(byte[] array) {
		_currentPageHasChanged = true;
		_buf.put(array);
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		_currentPageHasChanged = true;
		_buf.put((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		_currentPageHasChanged = true;
		_buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		_currentPageHasChanged = true;
		_buf.putChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		_currentPageHasChanged = true;
		_buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		_currentPageHasChanged = true;
		_buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		_currentPageHasChanged = true;
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		_currentPageHasChanged = true;
		_buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		_currentPageHasChanged = true;
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

	@Override
	public void noCheckRead(long[] array) {
		LongBuffer lb = _buf.asLongBuffer();
		lb.get(array);
	    _buf.position(_buf.position() + 8 * array.length);
	}
	
	@Override
	public void noCheckRead(int[] array) {
		IntBuffer lb = _buf.asIntBuffer();
		lb.get(array);
	    _buf.position(_buf.position() + 4 * array.length);
	}
	
	@Override
	public void noCheckWrite(long[] array) {
	    LongBuffer lb = _buf.asLongBuffer();
	    lb.put(array);
	    _buf.position(_buf.position() + 8 * array.length);
	}

	@Override
	public void noCheckWrite(int[] array) {
	    IntBuffer lb = _buf.asIntBuffer();
	    lb.put(array);
	    _buf.position(_buf.position() + 4 * array.length);
	}
	
	private void checkPosWrite(int delta) {
		if (isAutoPaging && _buf.position() + delta > MAX_POS) {
			int pageId = allocatePage();
			_buf.putInt(pageId);

			//write page
			writeData();
			_currentPageHasChanged = true; //??? TODO why not false?
			_currentPage = pageId;
			_buf.clear();
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
}
