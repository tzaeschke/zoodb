/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.jdo.internal.server;


import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.PagedPosIndex;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.server.index.SchemaIndex;
import org.zoodb.jdo.internal.server.index.SchemaIndex.SchemaIndexEntry;

/**
 * This class serves as a mediator between the serializer and the file access class.
 * Compared to the StorageWrite, the ObjectWriter also provides the following:
 * - Updating the oid- and class-index with new object positions.
 * - Insert the page header (currently containing only the class-oid).
 * 
 * SV stands for 'server'.
 * 
 * @author Tilmann Zaeschke
 */
public class ObjectWriterSV implements ObjectWriter, CallbackPageWrite {

	private final StorageChannelOutput out;
    private final PagedOidIndex oidIndex;
    private SchemaIndexEntry schemaIndexEntry;
    private final SchemaIndex schemaIndex;
    private final ZooClassDef def;
	private PagedPosIndex posIndex = null;
	private int currentPage = -1;
	private long currentOffs = -1;
	private final long headerForWrite;
	
	public ObjectWriterSV(StorageChannel file, PagedOidIndex oidIndex,
            ZooClassDef def, SchemaIndex schemaIndex) {
        this.out = file.getWriter(true);
        this.oidIndex = oidIndex;
        out.setOverflowCallbackWrite(this);
        this.def = def;
        this.headerForWrite = def.getOid();
        this.schemaIndex = schemaIndex;
    }

    @Override
	public void startObject(long oid, int prevSchemaVersion) {
        if (posIndex == null) {
            if (schemaIndexEntry == null) {
                schemaIndexEntry = schemaIndex.getSchema(def); 
            }
            posIndex = schemaIndexEntry.getObjectIndexLatestSchemaVersion();
        }
        PagedPosIndex prevIndex = schemaIndexEntry.getObjectIndexVersion(prevSchemaVersion);
		currentPage = out.getPage();
		currentOffs = out.getOffset();

        //first remove possible previous position
        final LLEntry objPos = oidIndex.findOidGetLong(oid);
        if (objPos != null) {
	        long pos = objPos.getValue(); //long with 32=page + 32=offs
	        //prevPos.getValue() returns > 0, so the loop is performed at least once.
	        do {
	            //remove and report to FSM if applicable
                //TODO
                //In cache, use separate list for evolved objects to be written (Map<PC/OID, OriginalClassDef)
                //Do not put those objects in dirty-list
                //When checking for dirty (external) return whether contained in dirty-list.
                //When...???
	        	
	            long nextPos = prevIndex.removePosLongAndCheck(pos);
	            //all secondary pages are marked.
	            nextPos |= PagedPosIndex.MARK_SECONDARY;
	            pos = nextPos;
	        } while (pos != PagedPosIndex.MARK_SECONDARY);
        }
        //Update pos index
        oidIndex.insertLong(oid, currentPage, (int)currentOffs);
	}
	
    @Override
	public void notifyOverflowWrite(int newPage) {
        //Update pos index
        posIndex.addPos(currentPage, currentOffs, newPage);
        currentPage = newPage;
        currentOffs = PagedPosIndex.MARK_SECONDARY;
    }

	@Override
	public void finishObject() {
	    posIndex.addPos(currentPage, currentOffs, 0);
	}
	
	/**
	 * This can be necessary when subsequent objects are of a different class.
	 */
	@Override
	public void newPage() {
		out.allocateAndSeekAP(DATA_TYPE.DATA, 0, headerForWrite);
		writeHeader();
	}

	private void writeHeader() {
		out.writeLong(headerForWrite);
	}
	
	@Override
	public void writeString(String string) {
		out.writeString(string);
	}

	@Override
	public void write(byte[] array) {
		out.write(array);
	}

	@Override
	public void writeBoolean(boolean boolean1) {
		out.writeBoolean(boolean1);
	}

	@Override
	public void writeByte(byte byte1) {
		out.writeByte(byte1);
	}

	@Override
	public void writeChar(char char1) {
		out.writeChar(char1);
	}

	@Override
	public void writeDouble(double double1) {
		out.writeDouble(double1);
	}

	@Override
	public void writeFloat(float float1) {
		out.writeFloat(float1);
	}

	@Override
	public void writeInt(int int1) {
		out.writeInt(int1);
	}

	@Override
	public void writeLong(long long1) {
		out.writeLong(long1);
	}

	@Override
	public void writeShort(short short1) {
		out.writeShort(short1);
	}

	@Override
	public void skipWrite(int nBytes) {
		out.skipWrite(nBytes);
	}

	@Override
	public void flush() {
		out.flush();
		//posIndex may change during next transaction due to schema evolution
		posIndex = null;
		//setting this to null is important for revert() on failed commits
		schemaIndexEntry = null;
	}

//    @Override
//    public String toString() {
//    	return "pos=" + file.getPage() + "/" + file.getOffset();
//    }
}
