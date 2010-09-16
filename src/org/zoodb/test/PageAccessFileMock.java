package org.zoodb.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;
import org.zoodb.jdo.internal.server.DiskAccessOneFile;
import org.zoodb.jdo.internal.server.PageAccessFile;

public class PageAccessFileMock implements SerialInput, SerialOutput, PageAccessFile {

	private ByteBuffer _buf;
	private int _currentPage = -1;
	private boolean _currentPageHasChanged = false;
	private int statNWrite = 0;
	
	private final ArrayList<ByteBuffer> _buffers = new ArrayList<ByteBuffer>();
	
	public PageAccessFileMock() {
		_buffers.add( ByteBuffer.allocateDirect(DiskAccessOneFile.PAGE_SIZE) );
		_buf = _buffers.get(0);
		_currentPage = 0;

		//TODO initialize header
		
		//fill buffer
	}

	public void seekPage(int pageId) {
		checkLocked();

		writeData();
		_currentPage = pageId;

		_buf = _buffers.get(pageId);
		_buf.rewind();
	}
	
	
	public void seekPage(int pageId, int pageOffset) {
		checkLocked();

		if (pageOffset < 0) {
			pageId--;
			pageOffset += DiskAccessOneFile.PAGE_SIZE;
		}
		if (pageId != _currentPage) {
			writeData();
			_currentPage = pageId;
			_buf = _buffers.get(pageId);
			_buf.rewind();
			//set limit to PAGE_SIZE, in case we were reading the last current page, or even
			//a completely new page.
			_buf.limit(DiskAccessOneFile.PAGE_SIZE);
		} else {
			_buf.rewind();
		}
		_buf.position(pageOffset);
	}
	
	public int allocateAndSeek() {
		checkLocked();

		writeData();
		int pageId = allocatePage();
		_currentPage = pageId;

		_buf.rewind();
		return pageId;
	}
	
	public int allocatePage() {
		statNWrite++;
		_buf = ByteBuffer.allocateDirect(DiskAccessOneFile.PAGE_SIZE);
		_buffers.add( _buf );
//		System.out.println("Allocating page ID: " + nPages);
		return _buffers.size()-1;
	}

	public void close() {
		flush();
//			_fc.force(true);
//			_fc.close();
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	public void flush() {
		writeData();
//			_fc.force(false);
	}
	
	//TODO remove this method
	public void writeData() {
		checkLocked();

		//TODO this flag needs only to be set after seek. I think. Remove updates in write methods.
		if (_currentPageHasChanged) {
			_buf.flip();
			//_fc.write(_buf, _currentPage * DiskAccessOneFile.PAGE_SIZE);
			_currentPageHasChanged = false;
		}
	}
	
	/**
	 * 
	 * @param page
	 * @deprecated ?? remove later ?
	 */
	public final void checkOverflow(int page) {
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

	public String readString(int xor) {
		checkLocked();
		int len = _buf.getInt(); //max 127
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char b = (char) (_buf.get() ^ xor);
			sb.append(b);
		}
		return sb.toString();
	}
	
	public String readString() {
		checkLocked();
		int len = _buf.getInt(); //max 127
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char b = (char) _buf.get();
			sb.append(b);
		}
		return sb.toString();
	}

	public void writeString(String string, int xor) {
		checkLocked();
		_buf.putInt(string.length()); //max 127
		for (int i = 0; i < string.length(); i++) {
			_buf.put((byte) (string.charAt(i) ^ xor));
		}
	}

	public void writeString(String string) {
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
		//TODO
		_currentPageHasChanged = true;
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		checkLocked();
		_currentPageHasChanged = true;
//		System.out.println("W_POS=" + _buf.position() + "/" + _fc.position());
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
