package org.zoodb.jdo.internal.server;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;

public class PageAccessFile_BB implements SerialInput, SerialOutput, PageAccessFile {

	private static final int S_BOOL = 1;
	private static final int S_BYTE = 1;
	private static final int S_CHAR = 2;
	private static final int S_DOUBLE = 8;
	private static final int S_FLOAT = 4;
	private static final int S_INT = 4;
	private static final int S_LONG = 8;
	private static final int S_SHORT = 2;
	
	private final File _file;
	private final ByteBuffer _buf;
	private int _currentPage = -1;
	private boolean _currentPageHasChanged = false;
	private final FileChannel _fc;
	
	private final AtomicInteger _lastPage = new AtomicInteger();
	private int statNWrite = 0;
	//indicate whether to automatically allocate and move to next page when page end is reached.
	private boolean isAutoPaging = false;
	private boolean isWriting = true;
	
	private final int PAGE_SIZE;
	private final int MAX_POS;
	
	public PageAccessFile_BB(String dbPath, String options, int pageSize) {
		PAGE_SIZE = pageSize;
		MAX_POS = PAGE_SIZE - 4;
		_file = new File(dbPath);
		if (!_file.exists()) {
			throw new JDOUserException("DB file does not exist: " + dbPath);
		}
		try {
    		RandomAccessFile _raf = new RandomAccessFile(_file, options);
    		_fc = _raf.getChannel();
    		if (_raf.length() == 0) {
    			_lastPage.set(-1);
    			isWriting = true;
    		} else {
    			int nPages = (int) Math.floor( (_raf.length()-1) / (long)PAGE_SIZE );
    			_lastPage.set(nPages);
    		}
    		_buf = ByteBuffer.allocateDirect(PAGE_SIZE);
    		_currentPage = 0;
    
    		//fill buffer
    		_buf.clear();
    		int n = _fc.read(_buf); 
    		if (n != PAGE_SIZE && _file.length() != 0) {
    			throw new JDOFatalDataStoreException("Bytes read: " + n);
    		}
		} catch (IOException e) {
		    throw new JDOFatalDataStoreException("Error opening database: " + dbPath, e);
		}
		while (_buf.hasRemaining()) {
			//fill buffer with '0'.
			_buf.put((byte)0);
		}
		_buf.limit(PAGE_SIZE);
		_buf.rewind();
	}

	public void seekPage(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;
		isWriting = false;
		try {
			writeData();
			_currentPage = pageId;
			_buf.clear();
			 _fc.read(_buf, pageId * PAGE_SIZE );
			_buf.rewind();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}
	
	
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;
        isWriting = false;
		try {
			if (pageId != _currentPage) {
				writeData();
				_currentPage = pageId;
				_buf.clear();
				_fc.read(_buf, pageId * PAGE_SIZE);
				//set limit to PAGE_SIZE, in case we were reading the last current page, or even
				//a completely new page.
				_buf.limit(PAGE_SIZE);
			} else {
				_buf.rewind();
			}
			_buf.position(pageOffset);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}
	
	public int allocateAndSeek(boolean autoPaging) {
		isAutoPaging = autoPaging;
        isWriting = true;
		int pageId = allocatePage();
		try {
			writeData();
			_currentPage = pageId;
			
			_buf.clear();
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
		return pageId;
	}
	
	private int allocatePage() {
		int nPages = _lastPage.addAndGet(1);
		return nPages;
	}

	@Override
	public void close() {
		try {
			flush();
			_fc.force(true);
			_fc.close();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error closing file: " + _file.getPath(), e);
		}
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	@Override
	public void flush() {
		try {
			writeData();
			_fc.force(false);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error writing page: " + _currentPage, e);
		}
	}
	
	private void writeData() {
		try {
			//TODO this flag needs only to be set after seek. I think. Remove updates in write methods.
		    //TODO replace with isWriting
			//The problem with isWriting is that there are (at least) two places that call seek()
			//and perform write afterwards: The DB initialization, and the main-page writer. They
			//should get a separate function.
			//if (isWriting) {
			if (_currentPageHasChanged) {
				statNWrite++;
				_buf.flip();
				_fc.write(_buf, _currentPage * PAGE_SIZE);
				_currentPageHasChanged = false;
			} else {
			    //writing an empty page?
			    //or writing to a page that was just read?
			    //TODO replace check for _currentPageHasChanged above and just check for position
			    // > 0??? What about half-read pages??
//			    if (_buf.position()!= 0) {
//			        throw new IllegalStateException();
//			    }
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error writing page: " + _currentPage, e);
		}
	}
	
	
	@Override
	public String readString() {
		checkPosRead(4);
		int len = _buf.getInt();
		byte[] ba = new byte[len];
		readFully(ba);
		return new String(ba);
	}

	
	@Override
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
		_currentPageHasChanged = true;
		
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
		_currentPageHasChanged = true;
		checkPosWrite(S_BYTE);
		_buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		_currentPageHasChanged = true;
		if (!checkPos(S_CHAR)) {
			write(ByteBuffer.allocate(S_CHAR).putChar(char1).array());
			return;
		}
		_buf.putChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		_currentPageHasChanged = true;
		if (!checkPos(S_DOUBLE)) {
			writeLong(Double.doubleToLongBits(double1));
			return;
		}
		_buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		_currentPageHasChanged = true;
		if (!checkPos(S_FLOAT)) {
			writeInt(Float.floatToIntBits(float1));
			return;
		}
		_buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		_currentPageHasChanged = true;
		if (!checkPos(S_INT)) {
			write(ByteBuffer.allocate(S_INT).putInt(int1).array());
			return;
		}
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		_currentPageHasChanged = true;
		if (!checkPos(S_LONG)) {
			write(ByteBuffer.allocate(S_LONG).putLong(long1).array());
			return;
		}
		_buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		_currentPageHasChanged = true;
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
			int pageId = _buf.getInt();
			try {
				_currentPage = pageId;
				_buf.clear();
				 _fc.read(_buf, pageId * PAGE_SIZE );
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
	public void assurePos(int currentPage, int currentOffs) {
		//TODO check Test_080_Serialization.largeObjects() with commit outside loop!
		if (currentPage != _currentPage || currentOffs != _buf.position()) {
			System.out.println("assurePos! *************************************************");
			if (true) throw new RuntimeException("cp="+currentPage+"/"+_currentPage+" co="+currentOffs + "/"+_buf.position());
			seekPage(currentPage, currentOffs, isAutoPaging);
		}
	}
	
	@Override
	public int statsGetWriteCount() {
		return statNWrite;
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
	public int getPageCount() {
		return _lastPage.get() + 1;
	}


	@Override
	public void setPageCount(int pageCount) {
		_lastPage.set(pageCount-1);
	}
}
