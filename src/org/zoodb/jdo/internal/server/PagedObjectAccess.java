package org.zoodb.jdo.internal.server;


import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.SerialOutput;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.PagedPosIndex;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;

/**
 * This class serves as a mediator between the serializer and the file access class.
 * 
 * @author Tilmann Zäschke
 */
public class PagedObjectAccess implements SerialOutput, SerialInput {

	private final PageAccessFile file;
	private final PagedOidIndex oidIndex;
	private PagedPosIndex posIndex;
	private final FreeSpaceManager fsm;
	private int currentPage = -1;
	private long currentOffs = -1;
	private long headerForWrite;
	
	public PagedObjectAccess(PageAccessFile file, PagedOidIndex oidIndex, FreeSpaceManager fsm) {
		this.file = file;
		this.oidIndex = oidIndex;
		this.fsm = fsm;
		file.setOverflowCallback(this);
	}

	void startObject(long oid) {
		currentPage = file.getPage();
		currentOffs = file.getOffset();

        //first remove possible previous position
        final LLEntry objPos = oidIndex.findOidGetLong(oid);
        if (objPos != null) {
	        long pos = objPos.getValue(); //long with 32=page + 32=offs
	        //prevPos.getValue() returns > 0, so the loop is performed at least once.
	        do {
	            //remove and report to FSM if applicable
//	            //TODO the 'if' is only necessary for the first entry, the other should be like the 
//	            //first
	            long nextPos = posIndex.removePosLongAndCheck(pos, fsm);
	            //all secondary pages are marked.
	            nextPos |= PagedPosIndex.MARK_SECONDARY;
	            pos = nextPos;
	        } while (pos != PagedPosIndex.MARK_SECONDARY);
        }
        //Update pos index
        oidIndex.insertLong(oid, currentPage, (int)currentOffs);
	}
	
    public void notifyOverflowWrite(int newPage) {
        //Update pos index
        posIndex.addPos(currentPage, currentOffs, newPage);
        currentPage = newPage;
        currentOffs = PagedPosIndex.MARK_SECONDARY;
        writeHeader();
    }

    public void notifyOverflowRead() {
        // skip header (schema OID)
        file.readLong();
    }

	public void finishObject() {
        posIndex.addPos(currentPage, currentOffs, 0);
	}
	
	/**
	 * This can be necessary when subsequent objects are of a different class.
	 */
	public void newPage(PagedPosIndex posIndex, long header) {
		file.allocateAndSeek(true, 0);
		this.posIndex = posIndex;
		this.headerForWrite = header;
		writeHeader();
	}

	private void writeHeader() {
	    file.writeLong(headerForWrite);
	}
	
	@Override
	public void writeString(String string) {
		file.writeString(string);
	}

	@Override
	public void write(byte[] array) {
		file.write(array);
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		file.writeBoolean(boolean1);
	}

	@Override
	public void writeByte(byte byte1) {
		file.writeByte(byte1);
	}

	@Override
	public void writeChar(char char1) {
		file.writeChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		file.writeDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		file.writeFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		file.writeInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		file.writeLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		file.writeShort(short1);
	}

	@Deprecated
	public long debugGetOffset() {
		return file.getOffset();
	}

	@Override
	public void skipWrite(int nBytes) {
		file.skipWrite(nBytes);
	}

    @Override
    public int readInt() {
        return file.readInt();
    }

    @Override
    public long readLong() {
        return file.readLong();
    }

    @Override
    public boolean readBoolean() {
        return file.readBoolean();
    }

    @Override
    public byte readByte() {
        return file.readByte();
    }

    @Override
    public char readChar() {
        return file.readChar();
    }

    @Override
    public double readDouble() {
        return file.readDouble();
    }

    @Override
    public float readFloat() {
        return file.readFloat();
    }

    @Override
    public short readShort() {
        return file.readShort();
    }

    @Override
    public void readFully(byte[] array) {
        file.readFully(array);
    }

    @Override
    public String readString() {
        return file.readString();
    }

    @Override
    public long readLongAtOffset(int offset) {
        return file.readLongAtOffset(offset);
    }

    @Override
    public void skipRead(int nBytes) {
        file.skipRead(nBytes);
    }

    @Override
    public void seekPos(long pageAndOffs, boolean autoPaging) {
        file.seekPos(pageAndOffs, autoPaging);
    }

    @Override
    public void seekPage(int page, int offs, boolean autoPaging) {
        file.seekPage(page, offs, autoPaging);
    }

    public long startReading(int page, int offs) {
        file.seekPage(page, offs, true);
        return file.readLongAtOffset(0);
    }
}
