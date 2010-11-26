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
	
	private final File _file;
	private final ByteBuffer _buf;
	private int _currentPage = -1;
	private boolean _currentPageHasChanged = false;
	private final FileChannel _fc;
	private boolean isReadOnly;
	
	private final AtomicInteger _lastPage = new AtomicInteger();
	private int statNWrite = 0;
	//indicate whether to automatically allocate and move to next page when page end is reached.
	private boolean isAutoPaging = false;
	
	public PageAccessFile_BB(File file, String options) throws IOException {
		_file = file;
		RandomAccessFile _raf = new RandomAccessFile(file, options);
		_fc = _raf.getChannel();
		int nPages = (int) Math.floor( _raf.length() / (long)DiskAccessOneFile.PAGE_SIZE ) -1;
		_lastPage.set(nPages);
//		_fc.lock();  //lock the file
		_buf = ByteBuffer.allocateDirect(DiskAccessOneFile.PAGE_SIZE);
		_currentPage = 0;

		//fill buffer
		_buf.clear();
		int n = _fc.read(_buf); 
		if (n != DiskAccessOneFile.PAGE_SIZE && file.length() != 0) {
			//System.err.println("Init: Bytes read: " + n);
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
		isReadOnly = true;
		try {
			writeData();
			_currentPage = pageId;
			_buf.clear();
			 _fc.read(_buf, pageId * DiskAccessOneFile.PAGE_SIZE );
			//set limit to PAGE_SIZE, in case we were reading the last current page.
//			_buf.limit(DiskAccessOneFile.PAGE_SIZE);
			_buf.rewind();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}
	
	
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;
		checkLocked();
		isReadOnly = true;
		try {
			if (pageId == 0) {
				new RuntimeException().printStackTrace();
			}
			if (pageOffset < 0) {
				pageId--;
				pageOffset += DiskAccessOneFile.PAGE_SIZE;
			}
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
		isReadOnly = false;
		try {
			writeData();
			_currentPage = pageId;
			
			_buf.clear();
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
		return pageId;
	}
	
	@Deprecated
	@Override
	public int allocatePage(boolean autoPaging) {
		isAutoPaging = autoPaging;
		isReadOnly = true;
		int nPages = _lastPage.addAndGet(1);
		return nPages;
	}

	private int allocatePage() {
		isReadOnly = true;
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
	
	/**
	 * 
	 * @param page
	 * @throws IOException
	 * @deprecated ?? remove later ?
	 */
	public final void checkOverflow(int page) throws IOException {
//		if (_raf.getFilePointer() >= (page+1) * DiskAccessOneFile.PAGE_SIZE) {
//			throw new IllegalStateException("Page overflow: " + 
//					(_raf.getFilePointer() - (page+1) * DiskAccessOneFile.PAGE_SIZE));
//		}
		//TODO remove, the buffer will throw an exception any.
		//-> keep it in case we use longer buffers???
		if (_buf.position() >= DiskAccessOneFile.PAGE_SIZE) {
			throw new IllegalStateException("Page overflow: " + _buf.position());
		}
	}

	
	@Override
	public String readString() {
		checkLocked();
		int len = _buf.getInt(); //max 127
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			checkPosWrite(S_BYTE); //TODO put outside loop
			char b = (char) _buf.get();
			sb.append(b);
		}
		System.out.println("char[] R -> " + _buf.position());
		return sb.toString();
	}

	
	@Override
	public void writeString(String string) {
		checkLocked();
		_buf.putInt(string.length()); //max 127
		for (int i = 0; i < string.length(); i++) {
			checkPosWrite(S_BYTE); //TODO put outside loop
			_buf.put((byte) string.charAt(i));
		}
		System.out.println("char[] W -> " + _buf.position());
	}

	@Override
	public boolean readBoolean() {
		checkLocked();
		checkPosRead(S_BOOL);
		return _buf.get() != 0;
	}

	@Override
	public byte readByte() {
		checkLocked();
		checkPosRead(S_BYTE);
		return _buf.get();
	}

	@Override
	public char readChar() {
		checkLocked();
		checkPosRead(S_CHAR);
		return _buf.getChar();
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
            System.out.println("gl=" + getLen + " A="+posA+ " l="+l+" buf=" + _buf.position());
            _buf.get(array, posA, getLen);
            posA += getLen;
            l -= getLen;
        }
//		_buf.get(array);
		System.out.println("Readin byte[]" + _buf.position());
	}

	@Override
	public int readInt() {
		checkLocked();
		checkPosRead(S_INT);
		return _buf.getInt();
	}

	@Override
	public long readLong() {
		checkLocked();
		checkPosRead(S_LONG);
		return _buf.getLong();
	}

	@Override
	public short readShort() {
		checkLocked();
		checkPosRead(S_SHORT);
		return _buf.getShort();
	}

	@Override
	public void write(byte[] array) {
		checkLocked();
		_currentPageHasChanged = true;
		
		checkPosWrite(4);
		
		
		
		int l = array.length;
		int posA = 0; //position in array
		while (l > 0) {
		    checkPosWrite(4);
		    int putLen = DiskAccessOneFile.PAGE_SIZE - _buf.position() - 4;
		    if (putLen > l) {
		        putLen = l;
		    }
		    System.out.println("pl=" + putLen + " A="+posA+ " l="+l+" buf=" + _buf.position());
		    _buf.put(array, posA, putLen);
		    posA += putLen;
		    l -= putLen;
		}
		
//        int posB = _buf.position();
//		while (l-posB + 4 >= DiskAccessOneFile.PAGE_SIZE) {
//		    int putLen = DiskAccessOneFile.PAGE_SIZE - posB - 4;
//	        _buf.put(array, posA, putLen);
//	        posA += putLen;
//	        l -= putLen;
//	        posB = 0;
//	        checkPosWrite(4+1);  //4+1 to trigger page break
//		}
//        int putLen = DiskAccessOneFile.PAGE_SIZE - posB - 4;
//        _buf.put(array, posA, putLen);
//        posA += putLen;
//        l -= putLen;
//        posB = 0;
//        checkPosWrite(4+1);  //4+1 to trigger page break
//		
////		_buf.put(array);
		System.out.println("byte[] -> " + _buf.position());
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_BOOL);
		_buf.put((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_BYTE);
		_buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_CHAR);
		_buf.putChar(char1);
	}

	@Override
	public void writeChars(String s) {
		checkLocked();
		if (s.length() > 0) {
			_currentPageHasChanged = true;
		}
		for (int i = 0; i < s.length(); i++) {
			//_buf.putChar(s.charAt(i));
			//TODO improve!!!
			checkPosWrite(S_CHAR);
			_buf.putChar(s.charAt(i));
			//TODO instead write chars until end of page, then get new page, then continue
		}
	}

	@Override
	public void writeDouble(double double1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_DOUBLE);
		_buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_FLOAT);
		_buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_INT);
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_LONG);
		_buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		checkLocked();
		_currentPageHasChanged = true;
		checkPosWrite(S_SHORT);
		_buf.putShort(short1);
	}
	
	private void checkPosWrite(int delta) {
		checkLocked();
		if (isAutoPaging && _buf.position() + delta + 4 > DiskAccessOneFile.PAGE_SIZE) {
			int pageId = allocatePage();
			//TODO remove
			System.out.println("Page overrun W: " + _currentPage + " -> " + pageId);
			isReadOnly = false;
			_buf.putInt(pageId);

			//write page
			writeData();
			_currentPage = pageId;
			_buf.clear();
		}
	}

	private void checkPosRead(int delta) {
		checkLocked();
		if (isAutoPaging && _buf.position() + delta + 4 > DiskAccessOneFile.PAGE_SIZE) {
			int pageId = _buf.getInt();
			//TODO remove
			System.out.println("Page overrun R: " + _currentPage + " -> " + pageId);
			isReadOnly = true;
			try {
				_currentPage = pageId;
				_buf.clear();
				if (pageId * DiskAccessOneFile.PAGE_SIZE <= 0) {
					throw new RuntimeException("pageid=" + pageId); //TODO remove
				}
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
	public void assurePos(int currentPage, int currentOffs) {
		if (currentPage != _currentPage || currentOffs != _buf.position()) {
			System.out.println("assurePos! *************************************************");
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

	//TODO implement read and write version of this buffer?!!!!! 
	private void checkReadOnly() {
		if (isReadOnly && _currentPage == 0) {
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
