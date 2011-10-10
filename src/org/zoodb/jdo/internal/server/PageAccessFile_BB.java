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
import java.nio.channels.OverlappingFileLockException;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOFatalUserException;
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
	
	private final ByteBuffer buf;
	private int currentPage = -1;
	private final FileChannel fc;
	private final RandomAccessFile raf;
	private final FileLock fileLock;
	
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
    		raf = new RandomAccessFile(file, options);
    		fc = raf.getChannel();
    		try {
    			//tryLock is supposed to return null, but it throws an Exception
    			fileLock = fc.tryLock();
    		} catch (OverlappingFileLockException e) {
       			fc.close();
    			raf.close();
    			throw new JDOFatalUserException(
    					"The file is already accessed by another process: " + dbPath);
    		}
    		isWriting = (raf.length() == 0);
    		buf = ByteBuffer.allocateDirect((int) PAGE_SIZE);
    		currentPage = 0;
    
    		//fill buffer
    		buf.clear();
    		int n = fc.read(buf); 
    		if (n != PAGE_SIZE && file.length() != 0) {
    			throw new JDOFatalDataStoreException("Bytes read: " + n);
    		}
		} catch (IOException e) {
		    throw new JDOFatalDataStoreException("Error opening database: " + dbPath, e);
		}
		buf.limit((int) PAGE_SIZE);
		buf.rewind();
	}

	private PageAccessFile_BB(FileChannel fc, long pageSize, FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		MAX_POS = (int) (PAGE_SIZE - 4);
		this.fc = fc;
		this.fsm = fsm;
		this.raf = null;
		this.fileLock = null;
		
		isWriting = false;
		buf = ByteBuffer.allocateDirect((int) PAGE_SIZE);
		currentPage = 0;
	}

	@Override
	public PageAccessFile split() {
		PageAccessFile_BB split = new PageAccessFile_BB(fc, PAGE_SIZE, fsm);
		splits.add(split);
		return split;
	}
	
	@Override
	public void seekPageForRead(int pageId, boolean autoPaging) {
	    seekPage(pageId, 0, autoPaging);
	}
	
	
	@Override
	public void seekPageForWrite(int pageId, boolean autoPaging) {
		isAutoPaging = autoPaging;
		writeData();
		isWriting = true;
		currentPage = pageId;
		buf.clear();
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
            if (isWriting) {
                writeData();
                isWriting = false;
            }
            if (pageId != currentPage) {
				currentPage = pageId;
				buf.clear();
				fc.read(buf, pageId * PAGE_SIZE);
			}
			buf.rewind();
			buf.position(pageOffset);
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
			currentPage = pageId;
			
			buf.clear();
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
			raf.close();
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
			throw new JDOFatalDataStoreException("Error writing page: " + currentPage, e);
		}
	}
	
	private void writeData() {
		try {
			if (isWriting) {
				statNWrite++;
				buf.flip();
				fc.write(buf, currentPage * PAGE_SIZE);
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error writing page: " + currentPage, e);
		}
	}
	
	
	@Override
	public String readString() {
		checkPosRead(4);
		int len = buf.getInt();

		//Align for 2-byte writing
		int p = buf.position();
		if ((p & 0x00000001) == 1) {
			buf.position(p+1);
		}

		char[] array = new char[len];
		CharBuffer cb = buf.asCharBuffer();
		int l = array.length;
        int posA = 0; //position in array
        while (l > 0) {
            checkPosRead(2);
            int getLen = MAX_POS - buf.position();
            getLen = getLen >> 1;
            if (getLen > l) {
                getLen = l;
            }
            cb = buf.asCharBuffer(); //create a buffer at the correct position
            cb.get(array, posA, getLen);
            buf.position(buf.position()+getLen*2);
            posA += getLen;
            l -= getLen;
        }
        return String.valueOf(array);
	}

	
	@Override
	public void writeString(String string) {
		checkPosWrite(4);
		buf.putInt(string.length());

		//Align for 2-byte writing
		int p = buf.position();
		if ((p & 0x00000001) == 1) {
			buf.position(p+1);
		}
		
		CharBuffer cb;
		int l = string.length();
		int posA = 0; //position in array
		while (l > 0) {
		    checkPosWrite(2);
		    int putLen = MAX_POS - buf.position();
		    putLen = putLen >> 1; //TODO loses odd values!
		    if (putLen > l) {
		        putLen = l;
		    }
		    //This is crazy!!! Unlike ByteBuffer, CharBuffer requires END as third param!!
		    cb = buf.asCharBuffer();
		    cb.put(string, posA, posA + putLen);
		    buf.position(buf.position() + putLen * 2);

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
		return buf.get();
	}

	@Override
	public char readChar() {
		if (!checkPos(S_CHAR)) {
			return readByteBuffer(S_CHAR).getChar();
		}
		return buf.getChar();
	}

	@Override
	public double readDouble() {
		if (!checkPos(S_DOUBLE)) {
			return Double.longBitsToDouble(readLong());
		}
		return buf.getDouble();
	}

	@Override
	public float readFloat() {
		if (!checkPos(S_FLOAT)) {
			return Float.intBitsToFloat(readInt());
		}
		return buf.getFloat();
	}

	@Override
	public void readFully(byte[] array) {
        int l = array.length;
        int posA = 0; //position in array
        while (l > 0) {
            checkPosRead(1);
            int getLen = MAX_POS - buf.position();
            if (getLen > l) {
                getLen = l;
            }
            buf.get(array, posA, getLen);
            posA += getLen;
            l -= getLen;
        }
	}

	@Override
	public void noCheckRead(long[] array) {
		LongBuffer lb = buf.asLongBuffer();
		lb.get(array);
	    buf.position(buf.position() + S_LONG * array.length);
	}
	
	@Override
	public void noCheckRead(int[] array) {
		IntBuffer lb = buf.asIntBuffer();
		lb.get(array);
	    buf.position(buf.position() + S_INT * array.length);
	}
	
	@Override
	public void noCheckRead(byte[] array) {
		buf.get(array);
	}
	
	@Override
	public int readInt() {
		if (!checkPos(S_INT)) {
			return readByteBuffer(S_INT).getInt();
		}
        return buf.getInt();
	}

	@Override
	public long readLong() {
		if (!checkPos(S_LONG)) {
			return readByteBuffer(S_LONG).getLong();
		}
		return buf.getLong();
	}

	@Override
	public short readShort() {
		if (!checkPos(S_SHORT)) {
			return readByteBuffer(S_SHORT).getShort();
		}
		return buf.getShort();
	}

	private ByteBuffer readByteBuffer(int len) {
		byte[] ba = new byte[len];
		readFully(ba);
		return ByteBuffer.wrap(ba);
	}
	
    @Override
    public long readLongAtOffset(int offset) {
        return buf.getLong(0);
    }

	@Override
	public void write(byte[] array) {
		int l = array.length;
		int posA = 0; //position in array
		while (l > 0) {
		    checkPosWrite(1);
		    int putLen = MAX_POS - buf.position();
		    if (putLen > l) {
		        putLen = l;
		    }
		    buf.put(array, posA, putLen);
		    posA += putLen;
		    l -= putLen;
		}
	}

	/**
	 * The no-check methods are thought to be faster, because they don't need range checking.
	 * Furthermore, they ensure that a page can be filled to the last byte. without a new page
	 * being allocated.
	 */
	@Override
	public void noCheckWrite(long[] array) {
	    LongBuffer lb = buf.asLongBuffer();
	    lb.put(array);
	    buf.position(buf.position() + S_LONG * array.length);
	}

	@Override
	public void noCheckWrite(int[] array) {
	    IntBuffer lb = buf.asIntBuffer();
	    lb.put(array);
	    buf.position(buf.position() + S_INT * array.length);
	}

	@Override
	public void noCheckWrite(byte[] array) {
	    buf.put(array);
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		writeByte((byte) (boolean1 ? 1 : 0));
	}

	@Override
	public void writeByte(byte byte1) {
		checkPosWrite(S_BYTE);
		buf.put(byte1);
	}

	@Override
	public void writeChar(char char1) {
		if (!checkPos(S_CHAR)) {
			write(ByteBuffer.allocate(S_CHAR).putChar(char1).array());
			return;
		}
		buf.putChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		if (!checkPos(S_DOUBLE)) {
			writeLong(Double.doubleToLongBits(double1));
			return;
		}
		buf.putDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		if (!checkPos(S_FLOAT)) {
			writeInt(Float.floatToIntBits(float1));
			return;
		}
		buf.putFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		if (!checkPos(S_INT)) {
			write(ByteBuffer.allocate(S_INT).putInt(int1).array());
			return;
		}
		buf.putInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		if (!checkPos(S_LONG)) {
			write(ByteBuffer.allocate(S_LONG).putLong(long1).array());
			return;
		}
		buf.putLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		if (!checkPos(S_SHORT)) {
			write(ByteBuffer.allocate(S_SHORT).putShort(short1).array());
			return;
		}
		buf.putShort(short1);
	}
	
	private boolean checkPos(int delta) {
		//TODO remove autopaging, the indices use anyway the noCheckMethods!!
		if (isAutoPaging) {
			return (buf.position() + delta - MAX_POS) <= 0;
		}
		return true;
	}

	private void checkPosWrite(int delta) {
		if (isAutoPaging && buf.position() + delta > MAX_POS) {
			int pageId = fsm.getNextPage(0);
			buf.putInt(pageId);

			//write page
			writeData();
			currentPage = pageId;
			buf.clear();
			
			if (overflowCallback != null) {
				overflowCallback.notifyOverflowWrite(currentPage);
			}
		}
	}

	private void checkPosRead(int delta) {
		if (isAutoPaging && buf.position() + delta > MAX_POS) {
			int pageId = buf.getInt();
			try {
				currentPage = pageId;
				buf.clear();
				 fc.read(buf, pageId * PAGE_SIZE );
				buf.rewind();
			} catch (IOException e) {
				throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
			}
			if (overflowCallback != null) {
			    overflowCallback.notifyOverflowRead();
			}
		}
 	}

    @Override
    public int getOffset() {
        return buf.position();
    }

    @Override
    public int getPage() {
        return currentPage;
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
	        int bPos = buf.position();
	        int putLen = MAX_POS - bPos;
	        if (putLen > l) {
	            putLen = l;
	        }
	        buf.position(bPos + putLen);
	        l -= putLen;
	    }
	}

	@Override
	public void skipRead(int nBytes) {
        int l = nBytes;
        while (l > 0) {
            checkPosRead(1);
            int bPos = buf.position();
            int putLen = MAX_POS - bPos;
            if (putLen > l) {
                putLen = l;
            }
            buf.position(bPos + putLen);
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
