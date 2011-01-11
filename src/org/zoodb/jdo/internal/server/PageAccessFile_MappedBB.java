package org.zoodb.jdo.internal.server;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;

public class PageAccessFile_MappedBB implements SerialInput, SerialOutput, PageAccessFile {

	private final FileChannel _fc;
	private MappedByteBuffer _buf;
	private final RandomAccessFile _raf;
	@Deprecated
	private final AtomicInteger _lastPage = new AtomicInteger();
	private int statNWrite = 0;
	private boolean isAutoPaging = false;
	private int _currentPage = -1;
	
	public PageAccessFile_MappedBB(File file, String options) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(file, options);
		_fc = raf.getChannel();
		if (raf.length() == 0) {
			_lastPage.set(-1);
		} else {
			int nPages = (int) Math.floor( (raf.length()-1) / (long)DiskAccessOneFile.PAGE_SIZE );
			_lastPage.set(nPages);
		}
//		int nPages = (int) Math.floor( _raf.length() / (long)DiskAccessOneFile.PAGE_SIZE ) + 1;
//		_lastPage.set(nPages);
		System.out.println("FILESIZE = " + raf.length() + "/" + file.length());
		_buf = raf.getChannel().map(MapMode.READ_WRITE, 0, 1024*1024*100);
		_raf = raf;
	}

	
	@Override
	public void seekPage(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;
		try { 
			_buf.position(pageId * DiskAccessOneFile.PAGE_SIZE);
			_currentPage = pageId;
		} catch (IllegalArgumentException e) {
			//TODO remove this stuff
			throw new IllegalArgumentException("Seek=" + pageId);
		}
	}
	
	
	@Override
	public void seekPage(int pageId, int pageOffset, boolean autoPaging) {
		isAutoPaging = autoPaging;
		_buf.position(pageId * DiskAccessOneFile.PAGE_SIZE + pageOffset);
        _currentPage = pageId;
	}
	
	
	@Override
	public String readString() {
		int len = _buf.getInt(); //max 127
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char b = (char) _buf.get();
			sb.append(b);
		}
		return sb.toString();
	}

	
	@Override
	public void writeString(String string) {
		_buf.putInt(string.length()); //max 127
		for (int i = 0; i < string.length(); i++) {
			_buf.put((byte) string.charAt(i));
		}
	}

	@Override
	public int allocateAndSeek(boolean autoPaging) {
		isAutoPaging = autoPaging; 
		statNWrite++;
		int pageId = _lastPage.addAndGet(1);
		_buf.position(pageId * DiskAccessOneFile.PAGE_SIZE);	
        _currentPage = pageId;
		return pageId;
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
		_buf.put(array);
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		check();
		_buf.put((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		check();
		_buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		check();
		_buf.putChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		check();
		_buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		check();
		_buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		check();
		_buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		check();
		_buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		check();
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

	
	@Override
	public int statsGetWriteCount() {
		return statNWrite;
	}


    @Override
    public int getPage() {
        return _currentPage;
    }
}
