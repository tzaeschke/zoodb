package org.zoodb.jdo.internal.server;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOUserException;

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
	
	private final ByteBuffer _buf;
	private int _currentPage = -1;
	private FileChannel fc; //TODO final
	private RandomAccessFile _raf; //TODO final
	private FileLock fileLock; //TODO final
	
	private final FreeSpaceManager fsm;
	private int statNWrite = 0;
	//indicate whether to automatically allocate and move to next page when page end is reached.
	private boolean isAutoPaging = false;
	private boolean isWriting = true;
	
	// use LONG to enforce long-arithmetic in calculations
	private final long PAGE_SIZE;
	private final int MAX_POS;
	
	private final List<PageAccessFile_BB> splits = new LinkedList<PageAccessFile_BB>();
	private PagedObjectAccess overflowCallback = null;
	
	public PageAccessFile_BB(String dbPath, String options, int pageSize, FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		MAX_POS = (int) (PAGE_SIZE - 4);
		this.fsm = fsm;
		File file = new File(dbPath);
		if (!file.exists()) {
			throw new JDOUserException("DB file does not exist: " + dbPath);
		}
		try {
    		_raf = new RandomAccessFile(file, options);
    		fc = _raf.getChannel();
    		fileLock = fc.lock();
    		isWriting = (_raf.length() == 0);
    		_buf = ByteBuffer.allocateDirect((int) PAGE_SIZE);
    		_currentPage = 0;
    
    		//fill buffer
    		_buf.clear();
    		int n = fc.read(_buf); 
    		if (n != PAGE_SIZE && file.length() != 0) {
    			throw new JDOFatalDataStoreException("Bytes read: " + n);
    		}
		} catch (IOException e) {
		    throw new JDOFatalDataStoreException("Error opening database: " + dbPath, e);
		}
		_buf.limit((int) PAGE_SIZE);
		_buf.rewind();
	}

	private PageAccessFile_BB(FileChannel fc, long pageSize, FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		MAX_POS = (int) (PAGE_SIZE - 4);
		this.fc = fc;
		this.fsm = fsm;
		this._raf = null;
		this.fileLock = null;
		
		isWriting = false;
		_buf = ByteBuffer.allocateDirect((int) PAGE_SIZE);
		_currentPage = 0;
	}

	@Override
	public PageAccessFile split() {
		PageAccessFile_BB split = new PageAccessFile_BB(fc, PAGE_SIZE, fsm);
		splits.add(split);
		return split;
	}
	
	@Override
	public void seekPageForRead(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;
		try {
			writeData();
			isWriting = false;
			_currentPage = pageId;
			_buf.clear();
			 fc.read(_buf, pageId * PAGE_SIZE );
			_buf.rewind();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}
	
	
	@Override
	public void seekPageForWrite(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;
		writeData();
		isWriting = true;
		_currentPage = pageId;
		_buf.clear();
	}
	
	@Override
	public void seekPos(long pageAndOffs, boolean autoPaging) {
		int page = BitTools.getPage(pageAndOffs);
		int offs = BitTools.getOffs(pageAndOffs);
		seekPage(page, offs, autoPaging);
	}

	@Override
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;
		try {
			if (pageId != _currentPage) {
				writeData();
		        isWriting = false;
				_currentPage = pageId;
				_buf.clear();
				fc.read(_buf, pageId * PAGE_SIZE);
				//set limit to PAGE_SIZE, in case we were reading the last current page, or even
				//a completely new page.
				_buf.limit((int) PAGE_SIZE);
			} else {
				_buf.rewind();
			}
			_buf.position(pageOffset);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}
	
	@Override
	public int allocateAndSeek(boolean autoPaging, int prevPage) {
		isAutoPaging = autoPaging;
		int pageId = fsm.getNextPage(prevPage);//allocatePage();
		try {
			writeData();
	        isWriting = true;
			_currentPage = pageId;
			
			_buf.clear();
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
		try {
			flush();
			fc.force(true);
			fileLock.release();
			fc.close();
			_raf.close();
			fc = null;
			_raf = null;
			fileLock = null;
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error closing database file.", e);
		}
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	@Override
	public void flush() {
		//flush associated splits.
		for (PageAccessFile_BB paf: splits) {
			//paf.flush(); //This made SerializerTest.largeObject 10 slower ?!!?!?!?
			paf.writeData();
			//To avoid unnecessary writing during the next flush()
			paf.isWriting = false;
		}
		try {
			writeData();
			isWriting = false;
			fc.force(false);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error writing page: " + _currentPage, e);
		}
	}
	
	private void writeData() {
		try {
			if (isWriting) {
				statNWrite++;
				_buf.flip();
				fc.write(_buf, _currentPage * PAGE_SIZE);
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error writing page: " + _currentPage, e);
		}
	}
	
	
	@Override
	public String readString() {
		checkPosRead(4);
		int len = _buf.getInt();

		//Align for 2-byte writing
		int p = _buf.position();
		if ((p & 0x00000001) == 1) {
			_buf.position(p+1);
		}

		char[] array = new char[len];
		CharBuffer cb = _buf.asCharBuffer();
		int l = array.length;
        int posA = 0; //position in array
        while (l > 0) {
            checkPosRead(2);
            int getLen = MAX_POS - _buf.position();
            getLen = getLen >> 1;
            if (getLen > l) {
                getLen = l;
            }
            cb = _buf.asCharBuffer(); //create a buffer at the correct position
            cb.get(array, posA, getLen);
            _buf.position(_buf.position()+getLen*2);
            posA += getLen;
            l -= getLen;
        }
        return String.valueOf(array);
	}

	
	@Override
	public void writeString(String string) {
		checkPosWrite(4);
		_buf.putInt(string.length());

		//Align for 2-byte writing
		int p = _buf.position();
		if ((p & 0x00000001) == 1) {
			_buf.position(p+1);
		}
		
		CharBuffer cb;
		int l = string.length();
		int posA = 0; //position in array
		while (l > 0) {
		    checkPosWrite(2);
		    int putLen = MAX_POS - _buf.position();
		    putLen = putLen >> 1; //TODO loses odd values!
		    if (putLen > l) {
		        putLen = l;
		    }
		    //This is crazy!!! Unlike ByteBuffer, CharBuffer requires END as third param!!
		    cb = _buf.asCharBuffer();
		    cb.put(string, posA, posA + putLen);
		    _buf.position(_buf.position() + putLen * 2);

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
	/**
	 * The no-check methods are thought to be faster, because they don't need range checking.
	 * Furthermore, they ensure that a page can be filled to the last byte. without a new page
	 * being allocated.
	 */
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
	
	private boolean checkPos(int delta) {
		//TODO remove autopaging, the indices use anyway the noCheckMethods!!
		if (isAutoPaging) {
			return (_buf.position() + delta - MAX_POS) <= 0;
		}
		return true;
	}

	private void checkPosWrite(int delta) {
		if (isAutoPaging && _buf.position() + delta > MAX_POS) {
			int pageId = fsm.getNextPage(0);
			_buf.putInt(pageId);

			//write page
			writeData();
			_currentPage = pageId;
			_buf.clear();
			
			if (overflowCallback != null) {
				overflowCallback.notifyOverflow(_currentPage);
			}
		}
	}

	private void checkPosRead(int delta) {
		if (isAutoPaging && _buf.position() + delta > MAX_POS) {
			int pageId = _buf.getInt();
			try {
				_currentPage = pageId;
				_buf.clear();
				 fc.read(_buf, pageId * PAGE_SIZE );
				_buf.rewind();
			} catch (IOException e) {
				throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
			}
		}
	}

    @Override
    public int getOffset() {
        return _buf.position();
    }

    @Override
    public int getPage() {
        return _currentPage;
    }
	
	@Override
	public int statsGetWriteCount() {
		int ret = statNWrite;
		for (PageAccessFile_BB p: splits) {
			ret += p.statNWrite;
		}
		return ret;
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
		return (int) PAGE_SIZE;
	}

	@Override
	public void setOverflowCallback(PagedObjectAccess overflowCallback) {
		this.overflowCallback = overflowCallback;
	}
}
