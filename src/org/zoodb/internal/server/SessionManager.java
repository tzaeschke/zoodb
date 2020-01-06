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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.internal.Node;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.server.index.SchemaIndex;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.RWSemaphore;
import org.zoodb.tools.ZooConfig;

/**
 * 
 * @author Tilmann Zaeschke
 */
class SessionManager {

	public static final Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

	private static final long ID_FAULTY_PAGE = Long.MIN_VALUE;

	private final FreeSpaceManager fsm;
	private final StorageRoot file;
	private final IOResourceProvider rootChannel;
	private final Path path;

	private final RootPage rootPage;
	private final int[] rootPages = new int[2];
	private int rootPageID = 0;
	private int txCount = 0;

	//hmm...
	private final SchemaIndex schemaIndex;
	private final PagedOidIndex oidIndex;

	private final StorageChannelOutput fileOut;

	private final RWSemaphore<DiskAccess> lock = new RWSemaphore<>();
	
	private final TxManager txManager;
	
	public SessionManager(Path path) {
		this.path = path;
		fsm = new FreeSpaceManager();
		file = createPageAccessFile(path, "rw", fsm);
		
		rootChannel = file.getIndexChannel();
		StorageChannelInput in = rootChannel.createReader(false);

		//read header
		FileHeader header = FileHeader.read(in);
		if (!header.successfulRead()) {
		    file.close();
		    throw DBLogger.newFatal(header.errorMsg().get(0));
		}
		rootPages[0] = header.getRootPages()[0];
		rootPages[1] = header.getRootPages()[1];
		rootPage = new RootPage();

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
			LOGGER.error(m);
			throw DBLogger.newFatal(m);
		}

		//readMainPage
		in.seekPageForRead(PAGE_TYPE.ROOT_PAGE, rootPages[rootPageID]);

		//read main directory (page IDs)
		//tx ID
		long txId = in.readLong();
		//User table 
		int userPage = in.readInt();
		//OID table
		int oidPage1 = in.readInt();
		//schemata
		int schemaPage1 = in.readInt();
		// TODO naming
		//indices
		this.txCount = in.readInt();
		//free space index
		int freeSpacePage = in.readInt();
		//page count (required for recovery of crashed databases)
		int pageCount = in.readInt();
		//last used oid - this may be larger than the last stored OID if the last object was deleted
		long lastUsedOid = in.readLong();
		
		rootChannel.dropReader(in);

		
		this.txManager = new TxManager(txCount * 2);

		
		//OIDs
		oidIndex = new PagedOidIndex(rootChannel, oidPage1, lastUsedOid);

		//dir for schemata
		schemaIndex = new SchemaIndex(rootChannel, schemaPage1, false);

		//free space index
		fsm.initBackingIndexLoad(rootChannel, freeSpacePage, pageCount);

		rootPage.set(userPage, oidPage1, schemaPage1, txCount, freeSpacePage, pageCount);

		fileOut = rootChannel.createWriter(false);
	}

	static FileHeader readHeader(Path path) {
	    StorageRoot file = createPageAccessFile(path, "rw", new FreeSpaceManager());
        IOResourceProvider rootChannel = file.getIndexChannel();
        StorageChannelInput in = rootChannel.createReader(false);
	    FileHeader header = FileHeader.read(in);
	    file.close();
	    return header;
	}
	
	/**
	 * Writes the main page.
	 * @param pageCount 
	 */
	private void writeMainPage(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage, int pageCount, StorageChannelOutput out, long lastUsedOid,
			long txId, int txCount) {
		rootPageID = (rootPageID + 1) % 2;
		
		out.seekPageForWrite(PAGE_TYPE.ROOT_PAGE, rootPages[rootPageID]);

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
		// TODO cast !!!! FIX
		this.txCount = (int) Math.max(txId, txCount + 1);
		out.writeInt(this.txCount);
		//free space index
		out.writeInt(freeSpaceIndexPage);
		//page count
		out.writeInt(pageCount);
		//last used oid
		out.writeLong(lastUsedOid);
		//tx ID. Writing the tx ID twice should ensure that the data between the two has been
		//written correctly.
		out.writeLong(txId);
		
		out.seekPageForWrite(PAGE_TYPE.ROOT_PAGE, rootPages[rootPageID]);

		// TODO remove or Safety:
		int oldRootPageId = (rootPageID + 1) % 2;
		int oldPageID = rootPages[oldRootPageId];
		StorageChannelInput in = rootChannel.createReader(false);
		in.seekPage(PAGE_TYPE.ROOT_PAGE, oldPageID, 0);
		long txOld = in.readLong();
//		if (txOld >= txId && txId > 5) {
//			new IllegalStateException("tx: " + txOld + " -> " + txId).printStackTrace();
//		}
	}
	
	private long checkRoot(StorageChannelInput in, int pageId) {
		in.seekPageForRead(PAGE_TYPE.ROOT_PAGE, pageId);
		long txID1 = in.readLong();
		//skip the data
//		for (int i = 0; i < 8; i++) {
//			in.readInt();
//		}
		// TODO cleanup
		
		in.readInt();
		in.readInt();
		in.readInt();
		int txCount = in.readInt();
		in.readInt();
		in.readInt();
		in.readLong();
		
		long txID2 = in.readLong();
		if (txID1 == txID2) {
			return txCount;
		}
		LOGGER.error("Main page is faulty: {}. Will recover from previous " +
				"page version.", pageId);
		return ID_FAULTY_PAGE;
	}

	public DiskAccessOneFile createSession(Node node, AbstractCache cache) {
		//Create the session first, because it locks the SessionManager!
		DiskAccessOneFile session = new DiskAccessOneFile(node, cache, this);
		if (file.getDataChannelCount() > 1) {
			txManager.setMultiSession();
		}
		return session;
	}
	
	private static StorageRoot createPageAccessFile(Path path, String options, 
			FreeSpaceManager fsm) {
		String dbPath = path.toString();
		try {
			Class<?> cls = Class.forName(ZooConfig.getFileProcessor());
			Constructor<?> con = cls.getConstructor(String.class, String.class, Integer.TYPE, 
					FreeSpaceManager.class);
			return (StorageRoot) con.newInstance(dbPath, options, ZooConfig.getFilePageSize(), fsm);
		} catch (InvocationTargetException e) {
			Throwable t2 = e.getCause();
			if (DBLogger.USER_EXCEPTION.isAssignableFrom(t2.getClass())) {
				throw (RuntimeException)t2;
			}
			throw DBLogger.newFatal("path=" + dbPath, e);
		} catch (Exception e) {
			throw DBLogger.newFatal("path=" + dbPath, e);
		}
	}
	
	void close(IOResourceProvider channel) {
		channel.close();
		if (file.getDataChannelCount() == 0) {
			LOGGER.info("Closing DB file: {}", path);
			file.close();
			SessionFactory.removeSession(this);
		}
	}

	Path getPath() {
		return path;
	}

	FreeSpaceManager getFsm() {
		return fsm;
	}

	StorageRoot getFile() {
		return file;
	}

	void commitInfrastructure(IOResourceProvider channel, int oidPage, int schemaPage1, long lastUsedOid, long txId) {
		int userPage = rootPage.getUserPage(); //not updated currently
		int indexPage = rootPage.getIndexPage(); //TODO remove this?

		//This needs to be written last, because it is updated by other write methods which add
		//new pages to the FSM.
		int freePage = channel.writeIndex(fsm::write);
		int pageCount = fsm.getPageCount();
		
		if (rootPage.isDirty(userPage, oidPage, schemaPage1, indexPage, freePage)) {
			this.txCount = (int) Math.max(txId, txCount + 1);
			rootPage.set(userPage, oidPage, schemaPage1, txCount, freePage, pageCount);
			
			// flush the file including all splits 
			channel.flush(); 
			writeMainPage(userPage, oidPage, schemaPage1, txCount, freePage, pageCount, fileOut, 
					lastUsedOid, txId, txCount);
			//Second flush to update root pages.
			channel.flush(); 
		}
		
		//tell FSM that new free pages can now be reused.
		fsm.notifyCommit();
		
		//refresh pos-index iterators, if any exist.
		//Refrshing iterators (pos-index etc) is not necessary at the moment.
		//refresh() is performed through the session object.
		//schemaIndex.refreshIterators();
		
		// TODO why do we deregister here?
		// Should we only deregister when we do commit(retain=false)?
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
	
	RWSemaphore<DiskAccess> getLock() {
		return lock;
	}

	void readLock(DiskAccess key) {
		lock.readLock(key);
	}

	void writeLock(DiskAccess key) {
		lock.writeLock(key);
	}

	void release(DiskAccess key) {
		lock.release(key);
	}

	/**
	 * @param isTrialRun Indicate whether the tx history should be updated or not. In trial runs, 
	 * the history is never updated.
	 * @return A list of conflicting objects or {@code null} if there are no conflicts
	 */
	List<Long> checkForConflicts(long txId, TxContext txContext, boolean isTrialRun) {
		return txManager.addUpdates(txId, txContext, isTrialRun);
	}

	TxManager getTxManager() {
		return txManager;
	}

	public boolean isLocked() {
		return lock.isLocked();
	}
	
	void startWriting(long txId) {
		// set index channel txid
		fsm.notifyBegin(txId);
	}
}
