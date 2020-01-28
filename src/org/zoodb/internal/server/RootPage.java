/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.server;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;

final class RootPage {

    private final int rootPageId;
    private long txId;
	private int userPage;
	private int oidPage;
	private int schemaPage;
	private int indexPage;
	private int freeSpaceIndexPage;
	private int pageCount;
    private long commitId;
    
    private long lastUsedOID;

    public static RootPage read(StorageChannelInput in, int pageId) {
        RootPage page = new RootPage(pageId);
        
        in.seekPageForRead(PAGE_TYPE.ROOT_PAGE, pageId);
        //read main directory (page IDs)
        //commit ID
        page.txId = in.readLong();
        //User table 
        page.userPage = in.readInt();
        //OID table
        page.oidPage = in.readInt();
        //schemata
        page.schemaPage = in.readInt();
        //indices
        page.indexPage = in.readInt();
        //free space index
        page.freeSpaceIndexPage = in.readInt();
        //page count (required for recovery of crashed databases)
        page.pageCount = in.readInt();
        //last used oid - this may be larger than the last stored OID if the last object was deleted
        page.lastUsedOID = in.readLong();

        // The idea is that we assume a page has been fully written if the first and last
        // part has been written (i.e. txIds are identical).
        long txId2 = in.readLong();
        if (page.txId != txId2) {
            SessionManager.LOGGER.error("Main page is faulty: {}. Will recover from previous " +
                    "page version.", pageId);
            page.commitId = SessionManager.ID_FAULTY_PAGE;
        }
        
        // TODO move inside protected block. This should still be save though,
        // at least much better than before. Issues #114/ #116
        page.commitId = in.readLong();
        // TODO remove with new DB format:
        // The database may not have a commitIf yet, i.e. 0.5.2 and earlier.
        // Note that this change is backwards compatible, a DB written with 0.5.3
        // can still be read with 0.5.2, but that will of course reintroduce the bug.
        if (page.commitId == 0) {
            page.commitId = page.txId;
        }
        
        System.out.println(
                "readRoot: page=" + page.rootPageId + 
                "  tx=" + page.txId + 
                "  user=" + page.userPage + 
                "  OID table=" + page.oidPage +
                "  schemata=" + page.schemaPage + 
                "  indices=" + page.indexPage +
                "  FSM=" + page.freeSpaceIndexPage +
                "  pCnt=" + page.pageCount +
                "  luOID=" + page.lastUsedOID +
                "  commit=" + page.commitId);
        
        return page;
    }

    private RootPage(int rootPageId) {
        this.rootPageId = rootPageId;
    }
    
    /**
     * Writes the main page.
     * @param commitId Id/count of the current commit.
     * @param txId Transaction ID.
     * @param out Write channel.
     */
    public void write(long commitId, long txId, StorageChannelOutput out) {
        out.seekPageForWrite(PAGE_TYPE.ROOT_PAGE, this.rootPageId);
        
        this.commitId = commitId;
        this.txId = txId;

        //tx ID
        out.writeLong(txId);
        //User table
        out.writeInt(userPage);
        //OID table
        out.writeInt(oidPage);
        //schemata
        out.writeInt(schemaPage);
        //indices
        out.writeInt(indexPage);
        //free space index
        out.writeInt(freeSpaceIndexPage);
        //page count
        out.writeInt(pageCount);
        //last used oid
        out.writeLong(lastUsedOID);
        //tx ID. Writing the tx ID twice should ensure that the data between the two has been
        //written correctly.
        out.writeLong(txId);
        // commit ID, TODO move inside block
        // TODO make block larger for future changes?
        out.writeLong(commitId);
        System.out.println(
                "writeRoot: page=" + rootPageId + 
                "  tx=" + txId + 
                "  user=" + userPage + 
                "  OID table=" + oidPage +
                "  schemata=" + schemaPage + 
                "  indices=" + indexPage +
                "  FSM=" + freeSpaceIndexPage +
                "  pCnt=" + pageCount +
                "  luOID=" + lastUsedOID +
                "  commit=" + commitId);
    }
    
	boolean hasChanged(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage) {
		if (this.userPage != userPage || 
				this.oidPage != oidPage || 
				this.schemaPage != schemaPage ||
				this.indexPage != indexPage ||
				this.freeSpaceIndexPage != freeSpaceIndexPage) {
			return true;
		}
		return false;
	}
	
	void set(int userPage, int oidPage, int schemaPage, int indexPage, long lastUsedOID,
			int freeSpaceIndexPage, int pageCount) {
		this.userPage = userPage;
		this.oidPage = oidPage;
		this.schemaPage = schemaPage;
		this.indexPage = indexPage;
		this.lastUsedOID = lastUsedOID;
		this.freeSpaceIndexPage = freeSpaceIndexPage;
		this.pageCount = pageCount;
	}

	@Deprecated
	int getUserPage() {
		return userPage; 
	}

	/**
	 * 
	 * @return Index page.
	 * @deprecated This should probably be removed. Are we gonna use this at some point?
	 */
	int getIndexPage() {
		return indexPage;
	}

	public int getSchemIndexPage() {
		return schemaPage;
	}

	public int getOidIndexPage() {
		return oidPage;
	}

	public int getFMSPage() {
		return freeSpaceIndexPage;
	}

	public int getFSMPageCount() {
		return pageCount;
	}
	
	public long getLastUsedOID() {
	    return lastUsedOID;
	}

    public long getCommitId() {
        return commitId;
    }

    public long getTxID() {
        return txId;
    }
}
