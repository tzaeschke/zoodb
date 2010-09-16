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

	private final File _file;
	private final ByteBuffer _buf;
	private int _currentPage = -1;
	private boolean _currentPageHasChanged = false;
	private final FileChannel _fc;
	private boolean isReadOnly;
	
	private final AtomicInteger _lastPage = new AtomicInteger();
	private int statNWrite = 0;
	
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

	public void seekPage(int pageId) {
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
	
	
	public void seekPage(int pageId, int pageOffset) {
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
	
	public int allocateAndSeek() {
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
	public int allocatePage() {
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

	public String readString() throws IOException {
		checkLocked();
		int len = _buf.getInt(); //max 127
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char b = (char) _buf.get();
			sb.append(b);
		}
		return sb.toString();
	}

	@Override
	public void writeString(String string) throws IOException {
		checkLocked();
		_buf.putInt(string.length()); //max 127
		for (int i = 0; i < string.length(); i++) {
			_buf.put((byte) string.charAt(i));
		}
	}

	@Override
	public boolean readBoolean() {
		checkLocked();
		return _buf.get() != 0;
	}

	@Override
	public byte readByte() {
		checkLocked();
		return _buf.get();
	}

	@Override
	public char readChar() {
		checkLocked();
		return _buf.getChar();
	}

	@Override
	public double readDouble() {
		checkLocked();
		return _buf.getDouble();
	}

	@Override
	public float readFloat() {
		checkLocked();
		return _buf.getFloat();
	}

	@Override
	public void readFully(byte[] array) {
		checkLocked();
		_buf.get(array);
	}

	@Override
	public int readInt() {
		checkLocked();
		return _buf.getInt();
	}

	@Override
	public long readLong() {
		checkLocked();
		return _buf.getLong();
	}

	@Override
	public short readShort() {
		checkLocked();
		return _buf.getShort();
	}

	@Override
	public void write(byte[] array) {
		checkLocked();
		_currentPageHasChanged = true;
		_buf.put(array);
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		checkLocked();
		_currentPageHasChanged = true;
		_buf.put((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		checkLocked();
		_currentPageHasChanged = true;
		_buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		checkLocked();
		_currentPageHasChanged = true;
		_buf.putChar(char1);
	}

	@Override
	public void writeChars(String s) {
		checkLocked();
		for (int i = 0; i < s.length(); i++) {
			_currentPageHasChanged = true;
			_buf.putChar(s.charAt(i));
		}
	}

	@Override
	public void writeDouble(double double1) {
		checkLocked();
		_currentPageHasChanged = true;
		_buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		checkLocked();
		_currentPageHasChanged = true;
		_buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		checkLocked();
		_currentPageHasChanged = true;
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		checkLocked();
		_currentPageHasChanged = true;
		_buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		checkLocked();
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
			seekPage(currentPage, currentOffs);
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
