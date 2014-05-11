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
package org.zoodb.internal.server;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.zoodb.internal.Node;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.server.index.SchemaIndex;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.tools.ZooConfig;

/**
 * 
 * @author Tilmann Zaeschke
 */
class SessionManager {

	private static final long ID_FAULTY_PAGE = Long.MIN_VALUE;

	private final FreeSpaceManager fsm;
	private final StorageChannel file;
	private final Path path;
	private int count = 0;

	private final RootPage rootPage;
	private final int[] rootPages = new int[2];
	private int rootPageID = 0;

	//hmm...
	private final SchemaIndex schemaIndex;
	private final PagedOidIndex oidIndex;

	private final StorageChannelOutput fileOut;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	private final TxManager txManager;
	
	public SessionManager(Path path) {
		this.path = path;
		fsm = new FreeSpaceManager();
		file = createPageAccessFile(path, "rw", fsm);
		
		StorageChannelInput in = file.getReader(false);

		//read header
		in.seekPageForRead(DATA_TYPE.DB_HEADER, 0);
		int fid = in.readInt();
		if (fid != DiskIO.DB_FILE_TYPE_ID) { 
			throw DBLogger.newFatal("Illegal File ID: " + fid);
		}
		int maj = in.readInt();
		int min = in.readInt();
		if (maj != DiskIO.DB_FILE_VERSION_MAJ) { 
			throw DBLogger.newFatal("Illegal major file version: " + maj + "." + min +
					"; Software version: " + 
					DiskIO.DB_FILE_VERSION_MAJ + "." + DiskIO.DB_FILE_VERSION_MIN);
		}
		if (min != DiskIO.DB_FILE_VERSION_MIN) { 
			throw DBLogger.newFatal("Illegal minor file version: " + maj + "." + min +
					"; Software version: " + 
					DiskIO.DB_FILE_VERSION_MAJ + "." + DiskIO.DB_FILE_VERSION_MIN);
		}

		int pageSize = in.readInt();
		if (pageSize != ZooConfig.getFilePageSize()) {
			//TODO actually, in this case would should just close the file and reopen it with the
			//correct page size.
			throw DBLogger.newFatal("Incompatible page size: " + pageSize);
		}
		
		//main directory
		rootPage = new RootPage();
		rootPages[0] = in.readInt();
		rootPages[1] = in.readInt();

		//check root pages
		//we have two root pages. They are used alternatingly.
		long r0 = checkRoot(in, rootPages[0]);
		long r1 = checkRoot(in, rootPages[1]);
		if (r0 > r1) {
			rootPageID = 0;
		} else {
			rootPageID = 1;
		}
		if (r0 == ID_FAULTY_PAGE && r1 == ID_FAULTY_PAGE) {
			String m = "Database is corrupted and cannot be recoverd. Please restore from backup.";
			DBLogger.severe(m);
			throw DBLogger.newFatal(m);
		}

		//readMainPage
		in.seekPageForRead(DATA_TYPE.ROOT_PAGE, rootPages[rootPageID]);

		//read main directory (page IDs)
		//tx ID
		long txId = in.readLong();
		this.txManager = new TxManager(this, txId);
		//User table 
		int userPage = in.readInt();
		//OID table
		int oidPage1 = in.readInt();
		//schemata
		int schemaPage1 = in.readInt();
		//indices
		int indexPage = in.readInt();
		//free space index
		int freeSpacePage = in.readInt();
		//page count (required for recovery of crashed databases)
		int pageCount = in.readInt();
		//last used oid - this may be larger than the last stored OID if the last object was deleted
		long lastUsedOid = in.readLong();
		
		//OIDs
		oidIndex = new PagedOidIndex(file, oidPage1, lastUsedOid);

		//dir for schemata
		schemaIndex = new SchemaIndex(file, schemaPage1, false);
		


		//free space index
		fsm.initBackingIndexLoad(file, freeSpacePage, pageCount);

		rootPage.set(userPage, oidPage1, schemaPage1, indexPage, freeSpacePage, pageCount);

		fileOut = file.getWriter(false);
	}

	/**
	 * Writes the main page.
	 * @param pageCount 
	 */
	private void writeMainPage(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage, int pageCount, StorageChannelOutput out, long lastUsedOid,
			long txId) {
		rootPageID = (rootPageID + 1) % 2;
		
		out.seekPageForWrite(DATA_TYPE.ROOT_PAGE, rootPages[rootPageID]);

		//**********
		// When updating this, also update checkRoot()!
		//**********
		
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
		out.writeLong(lastUsedOid);
		//tx ID. Writing the tx ID twice should ensure that the data between the two has been
		//written correctly.
		out.writeLong(txId);
	}
	
	private long checkRoot(StorageChannelInput in, int pageId) {
		in.seekPageForRead(DATA_TYPE.ROOT_PAGE, pageId);
		long txID1 = in.readLong();
		//skip the data
		for (int i = 0; i < 8; i++) {
			in.readInt();
		}
		long txID2 = in.readLong();
		if (txID1 == txID2) {
			return txID1;
		}
		DBLogger.severe("Main page is faulty: " + pageId + ". Will recover from previous " +
				"page version.");
		return ID_FAULTY_PAGE;
	}

	public DiskAccessOneFile createSession(Node node, AbstractCache cache) {
		count++;
		if (count > 1) {
			txManager.setMultiSession();
		}
		return new DiskAccessOneFile(node, cache, this, rootPage);
	}
	
	private static StorageChannel createPageAccessFile(Path path, String options, 
			FreeSpaceManager fsm) {
		String dbPath = path.toString();
		try {
			Class<?> cls = Class.forName(ZooConfig.getFileProcessor());
			Constructor<?> con = cls.getConstructor(String.class, String.class, Integer.TYPE, 
					FreeSpaceManager.class);
			StorageChannel paf = 
				(StorageChannel) con.newInstance(dbPath, options, ZooConfig.getFilePageSize(), fsm);
			return paf;
		} catch (Exception e) {
			if (e instanceof InvocationTargetException) {
				Throwable t2 = e.getCause();
				if (DBLogger.USER_EXCEPTION.isAssignableFrom(t2.getClass())) {
					throw (RuntimeException)t2;
				}
			}
			throw DBLogger.newFatal("path=" + dbPath, e);
		}
	}
	
	void close() {
		count--;
		if (count == 0) {
			DBLogger.debugPrintln(1, "Closing DB file: " + path);
			fsm.getFile().close();
			SessionFactory.removeSession(this);
		}
	}

	Path getPath() {
		return path;
	}

	FreeSpaceManager getFsm() {
		return fsm;
	}

	StorageChannel getFile() {
		return file;
	}

	void commitInfrastructure(int oidPage, int schemaPage1, long lastUsedOid, long txId) {
		int userPage = rootPage.getUserPage(); //not updated currently
		int indexPage = rootPage.getIndexPage(); //TODO remove this?

		//This needs to be written last, because it is updated by other write methods which add
		//new pages to the FSM.
		int freePage = fsm.write();
		int pageCount = fsm.getPageCount();
		
		if (!rootPage.isDirty(userPage, oidPage, schemaPage1, indexPage, freePage)) {
			return;
		}
		rootPage.set(userPage, oidPage, schemaPage1, indexPage, freePage, pageCount);
		
		// flush the file including all splits 
		file.flush(); 
		writeMainPage(userPage, oidPage, schemaPage1, indexPage, freePage, pageCount, fileOut, 
				lastUsedOid, txId);
		//Second flush to update root pages.
		file.flush(); 
		
		//tell FSM that new free pages can now be reused.
		fsm.notifyCommit();
		
		//refresh pos-index iterators, if any exist.
		//TODO not necessary at the moment..., all tests (e.g. Test_62) pass anyway.
		//refresh() is performed through the session object.
		//schemaIndex.refreshIterators();
		txManager.deRegisterTx(txId);
	}

	RootPage getRootPage() {
		return rootPage;
	}

	long getNextTxId() {
		return txManager.getNextTxId();
	}

	SchemaIndex getSchemaIndex() {
		return schemaIndex;
	}

	PagedOidIndex getOidIndex() {
		return oidIndex;
	}
	
	Lock getReadLock() {
		return lock.readLock();
	}
	
	Lock getWriteLock() {
		return lock.writeLock();
	}

	/**
	 * @return A list of conflicting objects or {@code null} if there are no conflicts
	 */
	List<Long> checkForConflicts(long txId, TxContext txContext) {
		return txManager.addUpdates(txId, txContext);
	}

	TxManager getTxManager() {
		return txManager;
	}
}
