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
	
	public PagedObjectAccess(PageAccessFile file, PagedOidIndex oidIndex, FreeSpaceManager fsm) {
		this.file = file;
		this.oidIndex = oidIndex;
		this.fsm = fsm;
	}

	void startWriting(long oid) {
        int page = file.getPage();
        int offs = file.getOffset();
        
        //first remove possible previous position
        LLEntry prevPos = oidIndex.findOidGetLong(oid);
        if (prevPos != null) {
        	long posPage = prevPos.getValue(); //pos with offs=0
            posIndex.removePosLong(posPage);
            //report to FSM
            if (!posIndex.containsPage(posPage)) {
				fsm.reportFreePage((int) (posPage >> 32));
			}
        }
        //Update pos index
        posIndex.addPos(page, offs, oid);
        oidIndex.insertLong(oid, page, offs);
	}
	
	
	/**
	 * This can be necessary when subsequent objects are of a different class.
	 */
	public void newPage(PagedPosIndex posIndex) {
		file.allocateAndSeek(true, 0);
		this.posIndex = posIndex;
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
		//_file.writeBoolean((byte) (boolean1 ? 1 : 0));
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
