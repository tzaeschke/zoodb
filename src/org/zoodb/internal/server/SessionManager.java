/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

	static final int ID_FAULTY_PAGE = Integer.MIN_VALUE;

	private final FreeSpaceManager fsm;
	private final StorageRoot file;
	private final Path path;

	private final RootPage rootPage;
	private final int[] rootPages;
	private int rootPageID;
	// This differs from tx-ID in that it is strictly increasing during commit.
	// Contrary to that, tx-IDs are strictly increasing during TX begin, but they
	// TXs may not commit in the order they start (and not all do commit).
	private long commitCount;

	private final SchemaIndex schemaIndex;
	private final PagedOidIndex oidIndex;
	private final StorageChannelOutput fileOut;
	private final RWSemaphore<DiskAccess> lock = new RWSemaphore<>();
	private final TxManager txManager;
	
	public SessionManager(Path path) {
		this.path = path;
		fsm = new FreeSpaceManager();
		file = createPageAccessFile(path, "rw", fsm);

		IOResourceProvider rootChannel = file.getIndexChannel();
		StorageChannelInput in = rootChannel.createReader(false);

		//read header
		FileHeader header = FileHeader.read(in);
		if (!header.successfulRead()) {
		    file.close();
		    throw DBLogger.newFatal(header.errorMsg().get(0));
		}
		this.rootPages = header.getRootPages();
		RootPage rootPage0 = RootPage.read(in, rootPages[0]);
		RootPage rootPage1 = RootPage.read(in, rootPages[1]);

		//check root pages
		//we have two root pages. They are used alternatingly.
		long r0 = rootPage0.getCommitId();
		long r1 = rootPage1.getCommitId();
		if (r0 > r1) {
			rootPageID = 0;
			this.rootPage = rootPage0;
		} else {
			rootPageID = 1;
			this.rootPage = rootPage1;
		}
		if (r0 == ID_FAULTY_PAGE && r1 == ID_FAULTY_PAGE) {
			String m = "Database is corrupted and cannot be recovered. Please restore from backup.";
			LOGGER.error(m);
			throw DBLogger.newFatal(m);
		}
		rootChannel.dropReader(in);

		RootPage root = rootPage;
		txManager = new TxManager(root.getTxID());
		commitCount = root.getCommitId();

		//OIDs
		oidIndex = new PagedOidIndex(rootChannel, root.getOidIndexPage(), root.getLastUsedOID());

		//dir for schemata
		schemaIndex = new SchemaIndex(rootChannel, root.getSchemaIndexPage(), false);

		//free space index
		fsm.initBackingIndexLoad(rootChannel, root.getFMSPage(), root.getFSMPageCount());

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
		// SessionManager removal is done indirectly via the SessionManagerFactory because it needs to happen
		// inside the SMF's synchronized block.
		if (SessionFactory.removeSessionIfUnused(this)) {
			LOGGER.info("Closing DB file: {}", path);
		}
	}

	/**
	 * @return 'true' if the SM was closed.
	 */
	boolean closeIfUnused() {
		return file.closeIfNoChannelsRemain();
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

	void commitInfrastructure(IOResourceProvider channel, int oidPage, int schemaPage,
	        long lastUsedOID, long txId) {
	    RootPage rootPage = getCurrentRootPage();
		int userPage = rootPage.getUserPage(); //not updated currently
		int indexPage = rootPage.getIndexPage(); //TODO remove this?

		//This needs to be written last, because it is updated by other write methods which add
		//new pages to the FSM.
		int freePage = channel.writeIndex(fsm::write);
		int pageCount = fsm.getPageCount();
		
		if (rootPage.hasChanged(userPage, oidPage, schemaPage, indexPage, freePage)) {
			// flush the file including all splits 
			channel.flush(); 
			// Switch to use other root page
	        rootPageID = (rootPageID + 1) % 2;
	        // This uniquely identifies the commit and imposes total ordering,
	        // which is, for example, used when opening a database and find the most recent commit.
	        commitCount++;
	        rootPage.set(userPage, oidPage, schemaPage, indexPage, lastUsedOID, freePage, pageCount);
			rootPage.write(commitCount, txId, fileOut, rootPages[rootPageID]);
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

	RootPage getCurrentRootPage() {
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

	void releaseLock(DiskAccess key) {
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
