package org.zoodb.jdo.internal.server;


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
public class PagedObjectAccess implements SerialOutput {

	private final PageAccessFile file;
	private final PagedOidIndex oidIndex;
	private PagedPosIndex posIndex;
	private final FreeSpaceManager fsm;
	private int currentPage = -1;
	private int currentOffs = -1;
	
	public PagedObjectAccess(PageAccessFile file, PagedOidIndex oidIndex, FreeSpaceManager fsm) {
		this.file = file;
		this.oidIndex = oidIndex;
		this.fsm = fsm;
	}

	void startWriting(long oid) {
		currentPage = file.getPage();
		currentOffs = file.getOffset();

        //first remove possible previous position
        final LLEntry objPos = oidIndex.findOidGetLong(oid);
        if (objPos != null) {
	        long pos = objPos.getValue(); //long with 32=page + 32=offs
	        //prevPos.getValue() returns > 0, so the loop is performed at least once.
	        do {
//	            //remove and report to FSM if applicable
//	            long nextPos = posIndex.removePosLong(pos);
//	            //TODO the 'if' is only necessary for the first entry, the other should be like the 
//	            //first
//	            if (!posIndex.containsPage(pos)) {
//					fsm.reportFreePage((int) (pos >> 32));
//				}
	            long nextPos = posIndex.removePosLongAndCheck(pos, fsm);
	            pos = nextPos;
	        } while (pos != 0);
        }
        //Update pos index
        oidIndex.insertLong(oid, currentPage, currentOffs);
	}
	
	public void notifyOverflow(int newPage) {
        //Update pos index
		long np = ((long)newPage) << 32L;
        posIndex.addPos(currentPage, currentOffs, np);
        currentPage = newPage;
        currentOffs = 0;
	}

	public void finishObject() {
        posIndex.addPos(currentPage, currentOffs, 0);
	}
	
	/**
	 * This can be necessary when subsequent objects are of a different class.
	 */
	public void newPage(PagedPosIndex posIndex) {
		file.allocateAndSeek(true, 0);
		this.posIndex = posIndex;
		file.setOverflowCallback(this);
	}

	public void finishPage() {
		file.setOverflowCallback(null);
	}
	
	@Override
	public void writeString(String string) {
		file.writeString(string);
	}

	public void close() {
		flush();
		file.flush();
		file.close();
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	public void flush() {
		file.flush();
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
}
