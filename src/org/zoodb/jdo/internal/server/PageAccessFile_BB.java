package org.zoodb.jdo.internal.server;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jdo.JDOFatalDataStoreException;

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
    private static final boolean DEBUG = false;  //TODO remove?
	
	private final File _file;
	private final ByteBuffer _buf;
	private int _currentPage = -1;
	private boolean _currentPageHasChanged = false;
	private final FileChannel _fc;
	
	private final AtomicInteger _lastPage = new AtomicInteger();
	private int statNWrite = 0;
	//indicate whether to automatically allocate and move to next page when page end is reached.
	private boolean isAutoPaging = false;
	
	public PageAccessFile_BB(File file, String options) throws IOException {
		_file = file;
		RandomAccessFile _raf = new RandomAccessFile(file, options);
		_fc = _raf.getChannel();
		if (_raf.length() == 0) {
			_lastPage.set(-1);
		} else {
			int nPages = (int) Math.floor( (_raf.length()-1) / (long)DiskAccessOneFile.PAGE_SIZE );
			_lastPage.set(nPages);
		}
		_buf = ByteBuffer.allocateDirect(DiskAccessOneFile.PAGE_SIZE);
		_currentPage = 0;

		//fill buffer
		_buf.clear();
		int n = _fc.read(_buf); 
		if (n != DiskAccessOneFile.PAGE_SIZE && file.length() != 0) {
			throw new JDOFatalDataStoreException("Bytes read: " + n);
		}
		while (_buf.hasRemaining()) {
			//fill buffer with '0'.
			_buf.put((byte)0);
		}
		_buf.limit(DiskAccessOneFile.PAGE_SIZE);
		_buf.rewind();
	}

	public void seekPage(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;
		checkLocked();
		try {
			writeData();
			_currentPage = pageId;
			_buf.clear();
			 _fc.read(_buf, pageId * DiskAccessOneFile.PAGE_SIZE );
			_buf.rewind();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}
	
	
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;
		checkLocked();
		try {
			if (pageId != _currentPage) {
				writeData();
				_currentPage = pageId;
				_buf.clear();
				_fc.read(_buf, pageId * DiskAccessOneFile.PAGE_SIZE);
				//set limit to PAGE_SIZE, in case we were reading the last current page, or even
				//a completely new page.
				_buf.limit(DiskAccessOneFile.PAGE_SIZE);
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
		int pageId = allocatePage();
		checkLocked();
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
		checkLocked();
		try {
			//TODO this flag needs only to be set after seek. I think. Remove updates in write methods.
			if (_currentPageHasChanged) {
				statNWrite++;
				_buf.flip();
				_fc.write(_buf, _currentPage * DiskAccessOneFile.PAGE_SIZE);
				_currentPageHasChanged = false;
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error writing page: " + _currentPage, e);
		}
	}
	
	
	@Override
	public String readString() {
		checkLocked();
		checkPosRead(4);
		int len = _buf.getInt();
		byte[] ba = new byte[len];
		readFully(ba);
        if (DEBUG) System.out.println("R-STR-Pos: " + _currentPage + "/" + (_buf.position()-4-ba.length) + "  Len: " + (new String(ba)).length() + "/" + len + " s=" + new String(ba)); //TODO
		return new String(ba);
	}

	
	@Override
	public void writeString(String string) {
		checkLocked();
		checkPosWrite(4);
        byte[] ba = string.getBytes();
        if (DEBUG) System.out.println("W-STR-Pos: " + _currentPage + "/" + (_buf.position()) + "  Len: " + string.length() + "/" + ba.length + " s=" + string); //TODO
		_buf.putInt(ba.length);
		write(ba);
	}

	@Override
	public boolean readBoolean() {
		checkLocked();
		checkPosRead(S_BOOL);
        boolean i = _buf.get() != 0;
        if (DEBUG) System.out.println("R-Pos: " + _currentPage + "/" + (_buf.position()-2) + "  Boolean: " + i); //TODO
        return i;
//		return _buf.get() != 0;
	}

	@Override
	public byte readByte() {
		checkLocked();
		checkPosRead(S_BYTE);
        byte i = _buf.get();
        if (DEBUG) System.out.println("R-Pos: " + _currentPage + "/" + (_buf.position()-1) + "  Byte: " + i); //TODO
        return i;
//		return _buf.get();
	}

	@Override
	public char readChar() {
		checkLocked();
		checkPosRead(S_CHAR);
        char i = _buf.getChar();
        if (DEBUG) System.out.println("R-Pos: " + _currentPage + "/" + (_buf.position()-2) + "  Char: " + i); //TODO
        return i;
//		return _buf.getChar();
	}

	@Override
	public double readDouble() {
		checkLocked();
		checkPosRead(S_DOUBLE);
		return _buf.getDouble();
	}

	@Override
	public float readFloat() {
		checkLocked();
		checkPosRead(S_FLOAT);
		return _buf.getFloat();
	}

	@Override
	public void readFully(byte[] array) {
		checkLocked();
        int l = array.length;
        int posA = 0; //position in array
        while (l > 0) {
            checkPosRead(4);
            int getLen = DiskAccessOneFile.PAGE_SIZE - _buf.position() - 4;
            if (getLen > l) {
                getLen = l;
            }
            _buf.get(array, posA, getLen);
            posA += getLen;
            l -= getLen;
        }
	}

	@Override
	public int readInt() {
		checkLocked();
		checkPosRead(S_INT);
		int i = _buf.getInt();
        if (DEBUG) System.out.println("R-Pos: " + _currentPage + "/" + (_buf.position()-4) + "  Int: " + i); //TODO
        return i;
//        return _buf.getInt();
	}

	@Override
	public long readLong() {
		checkLocked();
		checkPosRead(S_LONG);
        long i = _buf.getLong();
        if (DEBUG) System.out.println("R-Pos: " + _currentPage + "/" + (_buf.position()-S_LONG) + "  Long: " + i); //TODO
        return i;
//		return _buf.getLong();
	}

	@Override
	public short readShort() {
		checkLocked();
		checkPosRead(S_SHORT);
        short i = _buf.getShort();
        if (DEBUG) System.out.println("R-Pos: " + _currentPage + "/" + (_buf.position()-2) + "  Short: " + i); //TODO
        return i;
//		return _buf.getShort();
	}

	@Override
	public void write(byte[] array) {
		checkLocked();
		_currentPageHasChanged = true;
		
		int l = array.length;
		int posA = 0; //position in array
		while (l > 0) {
		    checkPosWrite(4);
		    int putLen = DiskAccessOneFile.PAGE_SIZE - _buf.position() - 4;
		    if (putLen > l) {
		        putLen = l;
		    }
		    _buf.put(array, posA, putLen);
		    posA += putLen;
		    l -= putLen;
		}
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_BOOL);
        if (DEBUG) System.out.println("Pos: " + _currentPage + "/" + _buf.position() + "  Bool: " + boolean1); //TODO
		_buf.put((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_BYTE);
        if (DEBUG) System.out.println("Pos: " + _currentPage + "/" + _buf.position() + "  Byte: " + byte1); //TODO
		_buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_CHAR);
        if (DEBUG) System.out.println("Pos: " + _currentPage + "/" + _buf.position() + "  Char: " + char1); //TODO
		_buf.putChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_DOUBLE);
        if (DEBUG) System.out.println("Pos: " + _currentPage + "/" + _buf.position() + "  Double: " + double1); //TODO
		_buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_FLOAT);
        if (DEBUG) System.out.println("Pos: " + _currentPage + "/" + _buf.position() + "  Float: " + float1); //TODO
		_buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_INT);
		if (DEBUG) System.out.println("Pos: " + _currentPage + "/" + _buf.position() + "  Int: " + int1); //TODO
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_LONG);
        if (DEBUG) System.out.println("Pos: " + _currentPage + "/" + _buf.position() + "  Long: " + long1); //TODO
		_buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_SHORT);
        if (DEBUG) System.out.println("W-Pos: " + _currentPage + "/" + _buf.position() + "  Short: " + short1); //TODO
		_buf.putShort(short1);
	}
	
	private void checkPosWrite(int delta) {
		checkLocked();
		if (isAutoPaging && _buf.position() + delta + 4 > DiskAccessOneFile.PAGE_SIZE) {
			int pageId = allocatePage();
			_buf.putInt(pageId);

			//write page
			writeData();
			_currentPageHasChanged = true;
			_currentPage = pageId;
			_buf.clear();
		}
	}

	private void checkPosRead(int delta) {
		checkLocked();
		if (isAutoPaging && _buf.position() + delta + 4 > DiskAccessOneFile.PAGE_SIZE) {
			int pageId = _buf.getInt();
			try {
				_currentPage = pageId;
				_buf.clear();
				 _fc.read(_buf, pageId * DiskAccessOneFile.PAGE_SIZE );
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
	
	/** @deprecated I guess this can be removed? */
	private boolean isLocked = false;
	private void checkLocked() {
		if (isLocked) {
			throw new IllegalStateException();
		}
	}

	@Override
	public void lock() {
		checkLocked(); //???
		isLocked = true;
	}

	@Override
	public void unlock() {
		isLocked = false;
	}

	@Override
	public int statsGetWriteCount() {
		return statNWrite;
	}
}
