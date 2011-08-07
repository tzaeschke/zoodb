package org.zoodb.jdo.internal.server;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;

public class PageAccessFile_NoBuf implements SerialInput, SerialOutput {

	private final RandomAccessFile _raf;
	private final File _file;
	
	private final AtomicInteger _lastPage = new AtomicInteger();
	
	private final int PAGE_SIZE;
	
	public PageAccessFile_NoBuf(File file, String options, int pageSize) throws IOException {
		PAGE_SIZE = pageSize;
		_file = file;
		_raf = new RandomAccessFile(file, options);
//		int nPages = (int) Math.floor( _raf.length() / (long)DiskAccessOneFile.PAGE_SIZE ) + 1;
//		_lastPage.set(nPages);
		if (_raf.length() == 0) {
			_lastPage.set(-1);
		} else {
			int nPages = (int) Math.floor( (_raf.length()-1) / (long)PAGE_SIZE );
			_lastPage.set(nPages);
		}
	}

	public void seekPage(int pageId) {
		try {
			_raf.seek(pageId * PAGE_SIZE);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}
	
	@Override
	public void seekPos(long pos, boolean autoPage) {
		//ignore autopageing??? TODO
		try {
			_raf.seek(pos);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Pos: " + pos, e);
		}
	}
	
	@Override
	public void seekPage(int pageId, int pageOffset, boolean autoPage) {
		//ignore autopageing??? TODO
		try {
			_raf.seek(pageId * PAGE_SIZE + pageOffset);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}
	
	
	@Override
	public String readString() {
	    try {
	        int len = _raf.read(); //max 127
	        StringBuilder sb = new StringBuilder(len);
	        for (int i = 0; i < len; i++) {
	            char b = (char) _raf.read();
	            sb.append(b);
	        }
	        return sb.toString();
	    } catch (IOException e) {
	        throw new JDOFatalDataStoreException("", e);
	    }
	}

	
	@Override
	public void writeString(String string) {
	    try {
	        _raf.write(string.length()); //max 127
	        for (int i = 0; i < string.length(); i++) {
	            _raf.write(string.charAt(i));
	        }
	    } catch (IOException e) {
	        throw new JDOFatalDataStoreException("", e);
	    }
	}

	public int allocatePage() throws IOException {
		int nPages = _lastPage.addAndGet(1);
//		System.out.println("Allocating page ID: " + nPages);
		return nPages;
	}

	public void close() {
		try {
			_raf.close();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error closing file: " + _file.getPath(), e);
		}
	}

	public void flush() {
		// TODO Auto-generated method stub
		System.out.println("STUB: PageAccessFile.flush()");
		//
	}
	
	@Override
	public boolean readBoolean() {
		try {
			return _raf.readBoolean();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public byte readByte() {
		try {
			return _raf.readByte();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public char readChar() {
		try {
			return _raf.readChar();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public double readDouble() {
		try {
			return _raf.readDouble();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public float readFloat() {
		try {
			return _raf.readFloat();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void readFully(byte[] array) {
		try {
			_raf.readFully(array);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public int readInt() {
		try {
			return _raf.readInt();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public long readLong() {
		try {
			return _raf.readLong();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public short readShort() {
		try {
			return _raf.readShort();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void write(byte[] array) {
		try {
			_raf.write(array);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		try {
			_raf.writeBoolean(boolean1);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void writeByte(byte byte1) {
		try {
			_raf.writeByte(byte1);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void writeChar(char char1) {
		try {
			_raf.writeChar(char1);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void writeDouble(double double1) {
		try {
			_raf.writeDouble(double1);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void writeFloat(float float1) {
		try {
			_raf.writeFloat(float1);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void writeInt(int int1) {
		try {
			_raf.writeInt(int1);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void writeLong(long long1) {
		try {
			_raf.writeLong(long1);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}

	@Override
	public void writeShort(short short1) {
		try {
			_raf.writeShort(short1);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}
	
	@Override
	public void skipWrite(int nBytes) {
		while (nBytes >= 8) {
			writeLong(0);
			nBytes -= 8;
		}
		while (nBytes >= 1) {
			writeByte((byte)0);
			nBytes -= 1;
		}
	}

	@Override
	public void skipRead(int nBytes) {
		while (nBytes >= 8) {
			readLong();
			nBytes -= 8;
		}
		while (nBytes >= 1) {
			readByte();
			nBytes -= 1;
		}
	}
}
