package org.zoodb.jdo.internal.server;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;

public class PageAccessFile_MappedBB implements SerialInput, SerialOutput, PageAccessFile {
//implements DataOutput, DataInput { 
//extends RandomAccessFile {

	private final FileChannel _fc;
	private final FileLock _fileLock;
	private MappedByteBuffer _buf;
	private final RandomAccessFile _raf;
	@Deprecated
	private boolean _currentPageHasChanged = false;
	private final AtomicInteger _lastPage = new AtomicInteger();
	private int statNWrite = 0;
	private boolean isAutoPaging = false;
	
	public PageAccessFile_MappedBB(File file, String options) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(file, options);
		_fc = raf.getChannel();
		_fileLock = null;//_fc.lock();  //TODO try lock and throw error otherwise
		int nPages = (int) Math.floor( raf.length() / (long)DiskAccessOneFile.PAGE_SIZE ) + 1;
		_lastPage.set(nPages);
		System.out.println("FILESIZE = " + raf.length() + "/" + file.length());
		_buf = raf.getChannel().map(MapMode.READ_WRITE, 0, 1024*1024*100);
		_raf = raf;
	}

	
	@Override
	public void seekPage(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;
		try { 
			_buf.position(pageId * DiskAccessOneFile.PAGE_SIZE);	
		} catch (IllegalArgumentException e) {
			//TODO remove this stuff
			throw new IllegalArgumentException("Seek=" + pageId);
		}
	}
	
	
	@Override
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;
		_buf.position(pageId * DiskAccessOneFile.PAGE_SIZE + pageOffset);
	}
	
	
	public String readString(int xor) throws IOException {
		int len = _buf.getInt(); //max 127
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char b = (char) (_buf.get() ^ xor);
			sb.append(b);
		}
		return sb.toString();
	}
	
	public String readString() throws IOException {
		int len = _buf.getInt(); //max 127
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char b = (char) _buf.get();
			sb.append(b);
		}
		return sb.toString();
	}

	public void writeString(String string, int xor) throws IOException {
		_buf.putInt(string.length()); //max 127
		for (int i = 0; i < string.length(); i++) {
			_buf.put((byte) (string.charAt(i) ^ xor));
		}
	}

	public void writeString(String string) throws IOException {
		_buf.putInt(string.length()); //max 127
		for (int i = 0; i < string.length(); i++) {
			_buf.put((byte) string.charAt(i));
		}
	}

	@Override
	public int allocateAndSeek(boolean autoPaging) {
		isAutoPaging = autoPaging; 
		statNWrite++;
		int pageId = allocatePage(autoPaging);
		_buf.position(pageId * DiskAccessOneFile.PAGE_SIZE);	
		return pageId;
	}

	@Override
	public int allocatePage(boolean autoPaging) {
		isAutoPaging = autoPaging;
		statNWrite++;
		int nPages = _lastPage.addAndGet(1);
//		System.out.println("Allocating page ID: " + nPages);
		return nPages;
	}

	public void close() {
		System.out.print("Closing DB file...");
		try {
			_buf.force();
			_fc.force(true);
			//TODO _fileLock.release();
			_fc.close();
			
			_raf.close();
			closeBruteForce();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error closing database file.", e);
		}
		System.out.println(" done.");
	}

	public void flush() {
		_buf.force();
	}
	
	private static final int GC_TIMEOUT_MS = 1000;
	
	private void closeBruteForce() {
		  WeakReference<MappedByteBuffer> bufferWeakRef = new WeakReference<MappedByteBuffer>(_buf);

		  _buf = null;

		  long start = System.currentTimeMillis();
		  while (bufferWeakRef.get() != null) {
			  if (System.currentTimeMillis() - start > GC_TIMEOUT_MS) {
				  throw new RuntimeException("Timeout (" + GC_TIMEOUT_MS + " ms) reached while trying to GC mapped buffer");
			  }
			  System.gc();
			  Thread.yield();
		  }
//		  try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
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
		if (_buf.position() >= (page+1) * DiskAccessOneFile.PAGE_SIZE) {
			throw new IllegalStateException("Page overflow: " + 
					(_buf.position() - (page+1) * DiskAccessOneFile.PAGE_SIZE));
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
		check();
		_currentPageHasChanged = true;
		_buf.put(array);
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		check();
		_currentPageHasChanged = true;
		_buf.put((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		check();
		_currentPageHasChanged = true;
		_buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		check();
		_currentPageHasChanged = true;
		_buf.putChar(char1);
	}

	@Override
	public void writeChars(String s) {
		check();
		for (int i = 0; i < s.length(); i++) {
			_currentPageHasChanged = true;
			_buf.putChar(s.charAt(i));
		}
	}

	@Override
	public void writeDouble(double double1) {
		check();
		_currentPageHasChanged = true;
		_buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		check();
		_currentPageHasChanged = true;
		_buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		check();
		_currentPageHasChanged = true;
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		check();
		_currentPageHasChanged = true;
		_buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		check();
		_currentPageHasChanged = true;
		_buf.putShort(short1);
	}

	private final void check() {
//		System.out.println("W_POS=" + _buf.position() + "/" + _fc.position());
//		if (_buf.position() > 20000) {
//			throw new IllegalStateException("Illegal POS=" + _buf.position());
//		}
	}
	

	@Override
	public int getOffset() {
		return _buf.position() % DiskAccessOneFile.PAGE_SIZE;
	}
	
	

	@Override
	public void assurePos(int currentPage, int currentOffs) {
		if (currentPage * DiskAccessOneFile.PAGE_SIZE + currentOffs != _buf.position()) {
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
