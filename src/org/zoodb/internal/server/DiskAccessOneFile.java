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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.DataDeSerializer;
import org.zoodb.internal.DataDeSerializerNoClass;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Node;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.ZooHandleImpl;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.internal.server.index.ObjectIterator;
import org.zoodb.internal.server.index.ObjectPosIterator;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.server.index.PagedOidIndex.FilePos;
import org.zoodb.internal.server.index.PagedPosIndex;
import org.zoodb.internal.server.index.SchemaIndex;
import org.zoodb.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.internal.server.index.ZooHandleIteratorAdapter;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.FormattedStringBuilder;
import org.zoodb.internal.util.PoolDDS;
import org.zoodb.internal.util.PrimLongMapLI;
import org.zoodb.internal.util.Util;
import org.zoodb.tools.DBStatistics.STATS;

/**
 * Disk storage functionality. This version stores all data in a single file, attempting a page 
 * based approach.
 * 
 * Caching
 * =======
 * Some data data is read on start-up and kept in memory:
 * - Schema index
 * - OID Index (all OIDs) -> needs to be changed
 *  
 * 
 * Page chaining
 * =============
 * All pages have a pointer to the following page of the same kind (data/schema/oids/...).
 * This pointer is allow bulk processing, e.g. deleting all data of one class (remove schema) or
 * performing a query without index.
 * 
 * Problem #1: when adding data, the previous page has to be updated
 * Solution: Always maintain a following empty page.
 * 
 * Problem #1B: Maintaining empty pages means that large objects can (usually) not be written in
 * consecutive pages.   
 * Solution: DIRTY: Use empty page only for small objects. For large objects, leave single page 
 * empty and append object data. OR: Allow fragmentation.
 * 
 *  Problem #2: How to find the empty page? Follow through all other pages? No!
 *  Solution #2: Keep a list of all the last pages for each data type. -> Could become a list of
 *               all empty pages.
 *               
 * Alternatives:
 * - Keep an index of all pages for a specific class.
 *   -> Abuse OID index as such an index?? -> Not very efficient.
 *   -> Keep one OID index per class??? Works well with few classes...
 * 
 * 
 * Advantages of paging:
 * - page allocation is possible, might be useful for parallel writes (?!)
 * - Many references are shorter (page ID is int), e.g. next_index_page in any index
 * - Also references from indices to objects could be reduced to page only, because the objects on
 *   the page are loaded anyway.
 *   TODO when loading all objects into memory, do not de-serialize them all! Deserialize only
 *   required objects on the loaded page, the others can be stored in a cache of byte[]!!!!
 *   -> Store OIDs + posInPage for all objects in a page in the beginning of that page.
 * 
 * 
 * @author Tilmann Zaeschke
 */
public class DiskAccessOneFile implements DiskAccess {
	
	private final Node node;
	private final AbstractCache cache;
	private final StorageChannel file;
	private final StorageChannelInput fileInAP;
	private final PoolDDS ddsPool;

	private final SchemaIndex schemaIndex;
	private final PagedOidIndex oidIndex;
	private final FreeSpaceManager freeIndex;
    private final ObjectReader objectReader;
	
    private final SessionManager sm;
    
    private long txId;
	private Lock lock;
	private final TxContext txContext = new TxContext(); 
	
	DiskAccessOneFile(Node node, AbstractCache cache, SessionManager sm, RootPage rp) {
		this.sm = sm;
		this.node = node;
		this.cache = cache;

		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		//TODO read-lock
		lock = sm.getWriteLock();
		lock.lock();
		
		this.freeIndex = sm.getFsm();
		this.file = sm.getFile();
		
		
		//OIDs
		oidIndex = sm.getOidIndex();

		//dir for schemata
		schemaIndex = sm.getSchemaIndex();
		
        objectReader = new ObjectReader(file);
		
		ddsPool = new PoolDDS(file, this.cache);

		fileInAP = file.getReader(true);
	}
	
	@Override
	public void refreshSchema(ZooClassDef def) {
		schemaIndex.refreshSchema(def, this);
	}

	
	/**
	 * @return List of all schemata in the database. These are loaded when the database is opened.
	 */
	@Override
	public Collection<ZooClassDef> readSchemaAll() {
		Collection<ZooClassDef> all = schemaIndex.readSchemaAll(this, node);
		if (all.isEmpty()) {
			//new database, need to initialize!
			
			//This is the root schema
			ZooClassDef zpcDef = ZooClassDef.bootstrapZooPCImpl(); 
			ZooClassDef meta = ZooClassDef.bootstrapZooClassDef(); 
			schemaIndex.defineSchema(zpcDef);
			schemaIndex.defineSchema(meta);

			all = new ArrayList<ZooClassDef>();
			all.add(zpcDef);
			all.add(meta);
		}
		txContext.setSchemaTxId(schemaIndex.getTxIdOfLastWrite());
		return all;
	}


	@Override
	public void newSchemaVersion(ZooClassDef defOld, ZooClassDef defNew) {
		schemaIndex.newSchemaVersion(defOld, defNew);
	}

	@Override
	public void defineSchema(ZooClassDef def) {
		schemaIndex.defineSchema(def);
	}

	@Override
	public void undefineSchema(ZooClassProxy def) {
		dropInstances(def);
		schemaIndex.undefineSchema(def);
	}

	@Override
	public void renameSchema(ZooClassDef def, String newName) {
		schemaIndex.renameSchema(def, newName);
	}

	@Override
	public void deleteSchema(ZooClassDef sch) {
		schemaIndex.deleteSchema(sch);
	}

	@Override
	public long[] allocateOids(int oidAllocSize) {
		long[] ret = oidIndex.allocateOids(oidAllocSize);
		return ret;
	}
		
	@Override
	public void dropInstances(ZooClassProxy def) {
	    //ensure latest
	    SchemaIndexEntry sie = schemaIndex.getSchema(def.getSchemaId());
	    for (int i = 0; i < sie.getObjectIndexVersionCount(); i++) {
	        PagedPosIndex oi = sie.getObjectIndexVersion(i);
    		PagedPosIndex.ObjectPosIterator it = oi.iteratorObjects();
    		
    		//clean oid index
    		DataDeSerializerNoClass dds = new DataDeSerializerNoClass(fileInAP);
    		while (it.hasNextOPI()) {
    			long pos = it.nextPos();
    			//simply remove all pages
    			freeIndex.reportFreePage(BitTools.getPage(pos));
    			
    			dds.seekPos(pos);
    			//first read the key, then afterwards the field!
    			long oid = dds.getOid();
    			oidIndex.removeOidNoFail(oid, -1); //value=long with 32=page + 32=offs
    			txContext.addOidUpdate(oid, txId);
    		}
    		it.close();
    		
    		//clean field indices
    		sie.clearIndices();
    		
    		//clean pos index
    		oi.clear();
		}
	}
	
	@Override
	public SchemaIndexEntry getSchemaIE(ZooClassDef def) {
	    return schemaIndex.getSchema(def);
	}
	
	@Override
	public PagedOidIndex getOidIndex() {
	    return oidIndex;
	}
	
	@Override
	public long countInstances(ZooClassProxy clsDef, boolean subClasses) {
		return schemaIndex.countInstances(clsDef, subClasses);
	}

	@Override
	public ObjectWriter getWriter(ZooClassDef def) {
	    return new ObjectWriterSV(file, oidIndex, def, schemaIndex);
	}
	
	/**
	 * Read objects.
	 * This should never be necessary. -> add warning?
	 * -> Only required for queries without index, which is worth a warning anyway.
	 */
	@Override
	public CloseableIterator<ZooPC> readAllObjects(long schemaId, boolean loadFromCache) {
		SchemaIndexEntry se = schemaIndex.getSchema(schemaId);
		if (se == null) {
			throw DBLogger.newUser("Schema not found for class: " + schemaId);
		}
		
		return new ObjectPosIterator(se.getObjectIndexIterator(), cache, objectReader, 
		        loadFromCache);
	}
	
	/**
	 * WARNING: float/double values need to be converted with BitTools before used on indices. 
	 */
	@Override
	public CloseableIterator<ZooPC> readObjectFromIndex(
			ZooFieldDef field, long minValue, long maxValue, boolean loadFromCache) {
		SchemaIndexEntry se = schemaIndex.getSchema(field.getDeclaringType());
		LongLongIndex fieldInd = (LongLongIndex) se.getIndex(field);
		LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> iter = fieldInd.iterator(minValue, maxValue);
		return new ObjectIterator(iter, cache, this, objectReader, loadFromCache);
	}	
	
    /**
     * Read objects.
     * This should never be necessary. -> add warning?
     * -> Only required for queries without index, which is worth a warning anyway.
     */
    @Override
    public CloseableIterator<ZooHandleImpl> oidIterator(ZooClassProxy clsPx, boolean subClasses) {
        SchemaIndexEntry se = schemaIndex.getSchema(clsPx.getSchemaId());
        if (se == null) {
            throw new IllegalStateException("Schema not found for class: " + clsPx);
        }

        ZooHandleIteratorAdapter it = new ZooHandleIteratorAdapter(
                se.getObjectIndexIterator(), objectReader, cache);
        return it;
    }
    	
	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	@Override
	public ZooPC readObject(long oid) {
	    final DataDeSerializer dds = ddsPool.get();
		final ZooPC pci = readObject(dds, oid);
		ddsPool.offer(dds);
		return pci;
	}

	/**
	 * Locate an object.
	 * @param pc
	 */
	@Override
	public void readObject(ZooPC pc) {
		long oid = pc.jdoZooGetOid();
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw DBLogger.newObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
		}
		
		try {
	        final DataDeSerializer dds = ddsPool.get();
            dds.readObject(pc, oie.getPage(), oie.getOffs());
	        ddsPool.offer(dds);
		} catch (Exception e) {
			throw DBLogger.newObjectNotFoundException(
					"ERROR reading object: " + Util.oidToString(oid), e);
		}
	}

	@Override
	public GenericObject readGenericObject(ZooClassDef def, long oid) {
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw DBLogger.newObjectNotFoundException(
					"ERROR OID not found: " + Util.oidToString(oid));
		}
		
		GenericObject go;
		try {
	        final DataDeSerializer dds = ddsPool.get();
            go = dds.readGenericObject(oie.getPage(), oie.getOffs());
	        ddsPool.offer(dds);
		} catch (Exception e) {
			throw DBLogger.newObjectNotFoundException(
					"ERROR reading object: " + Util.oidToString(oid), e);
		}
		return go;
	}

	
	/**
	 * Locate an object. This version allows providing a data de-serializer. This will be handy
	 * later if we want to implement some concurrency, which requires using multiple of the
	 * stateful DeSerializers. 
	 * @param dds
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	@Override
	public ZooPC readObject(DataDeSerializer dds, long oid) {
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw DBLogger.newObjectNotFoundException("OID not found: " + Util.oidToString(oid));
		}
		
		return dds.readObject(oie.getPage(), oie.getOffs(), false);
	}

	@Override
	public boolean checkIfObjectExists(long oid) {
		FilePos oie = oidIndex.findOid(oid);
		return oie != null;
	}

	@Override
	public void close() {
		DBLogger.debugPrintln(1, "Closing DB session: " + node.getDbPath());
		sm.close();
	}

	@Override
	public long beginTransaction() {
		txContext.reset();
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO Use R-Lock
		//TODO
		//TODO Move locking code into SessionManager
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		lock = sm.getWriteLock();
		//lock.lock();
		try {
			if (!lock.tryLock(10, TimeUnit.SECONDS)) {
				throw DBLogger.newFatal("Deadlock?");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		txId = sm.getNextTxId();
		return txId;
	}
	
	@Override
	public void rollbackTransaction() {
		try {
			//anything to do here?
			//--> This is also used during start-up to drop locks on the SessionManager!
		} finally {
			lock.unlock();
			lock = null;
		}
	}
	
	@Override
	public List<Long> checkTxConsistency(ArrayList<Long> updateOid, 
			ArrayList<Long> updateTimestamps) {
		//change read-lock to write-lock
		lock.unlock();
		lock = sm.getWriteLock();
		lock.lock();

		if (txContext.getSchemaTxId() != schemaIndex.getTxIdOfLastWrite()) {
			throw DBLogger.newUser("Database schema has changed, please reconnect.");
		}
		txContext.addOidUpdates(updateOid, updateTimestamps);
		List<Long> conflicts = sm.checkForConflicts(txId, txContext, true);
		txContext.reset();
		if (conflicts != null) {
			return conflicts;
		}

		//change write-lock to read-lock
		lock.unlock();
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//lock = sm.getReadLock();
		lock = sm.getWriteLock();
		lock.lock();

		
		return Collections.emptyList();
	}

	@Override
	public List<Long> beginCommit(ArrayList<Long> updateOid, ArrayList<Long> updateTimestamps) {
		//change read-lock to write-lock
		lock.unlock();
		lock = sm.getWriteLock();
		lock.lock();

		if (txContext.getSchemaTxId() != schemaIndex.getTxIdOfLastWrite()) {
			throw DBLogger.newUser("Database schema has changed, please reconnect. Expected " + 
					txContext.getSchemaTxId() + " but got " + schemaIndex.getTxIdOfLastWrite());
		}
		txContext.addOidUpdates(updateOid, updateTimestamps);
		List<Long> conflicts = sm.checkForConflicts(txId, txContext, false);
		if (conflicts != null) {
			return conflicts;
		}
		
		file.newTransaction(txId);
		freeIndex.notifyBegin(txId);

		return Collections.emptyList();
	}

	@Override
	public void commit() {
		try {
			int oidPage = oidIndex.write();
			int schemaPage1 = schemaIndex.write(txId);
			txContext.setSchemaTxId(schemaIndex.getTxIdOfLastWrite());
			
			sm.commitInfrastructure(oidPage, schemaPage1, oidIndex.getLastUsedOid(), txId);
			txContext.reset();
		} finally {
			lock.unlock();
			lock = null;
		}
	}

	/**
	 * This can be called after a failed commit (or JDO flush()). In case the root pages have not 
	 * been rewritten, this method will revert existing changes.
	 */
	@Override
	public void revert() {
		//We do NOT need a new txId here, revert() is just called when commit() fails.

		//Empty file buffers. For now we just flush them.
		file.flush(); //TODO revert for file???
		
		RootPage rootPage = sm.getRootPage();
		//revert --> back to previous (expected) schema-tx-ID
		schemaIndex.revert(rootPage.getSchemIndexPage(), txContext.getSchemaTxId());
		//We use the historic page count to avoid page-leaking
		freeIndex.revert(rootPage.getFMSPage(), rootPage.getFSMPageCount());
		//We do NOT reset the OID count. That may cause OID leaking(does it?), but the OIDs are
		//still assigned to uncommitted objects.
		oidIndex.revert(rootPage.getOidIndexPage());
	}
	
	/**
	 * Defines an index and populates it. All objects are put into the cache. This is not 
	 * necessarily useful, but it is a one-off operation. Otherwise we would need a special
	 * purpose implementation of the deserializer, which would have the need for a cache removed.
	 */
	@Override
	public void defineIndex(ZooClassDef def, ZooFieldDef field, boolean isUnique) {
		SchemaIndexEntry se = schemaIndex.getSchema(def);
		LongLongIndex fieldInd = (LongLongIndex) se.defineIndex(field, isUnique);
		
		//fill index with existing objects
		PagedPosIndex ind = se.getObjectIndexLatestSchemaVersion();
		PagedPosIndex.ObjectPosIterator iter = ind.iteratorObjects();
        DataDeSerializerNoClass dds = new DataDeSerializerNoClass(fileInAP);
        if (field.isPrimitiveType()) {
			while (iter.hasNext()) {
				long pos = iter.nextPos();
				dds.seekPos(pos);
				//first read the key, then afterwards the field!
				long key = dds.getAttrAsLong(def, field);
				if (isUnique) {
					if (!fieldInd.insertLongIfNotSet(key, dds.getLastOid())) {
						throw DBLogger.newUser("Duplicate entry in unique index: " +
								Util.oidToString(dds.getLastOid()));
					}
				} else {
					fieldInd.insertLong(key, dds.getLastOid());
				}
			}
        } else {
			while (iter.hasNext()) {
				long pos = iter.nextPos();
				dds.seekPos(pos);
				//first read the key, then afterwards the field!
				long key = dds.getAttrAsLongObjectNotNull(def, field);
				fieldInd.insertLong(key, dds.getLastOid());
				//TODO handle null values:
				//-ignore them?
				//-use special value?
			}
			//DatabaseLogger.debugPrintln(0, "FIXME defineIndex()");
        }
        iter.close();
	}

	@Override
	public boolean removeIndex(ZooClassDef cls, ZooFieldDef field) {
		SchemaIndexEntry e = schemaIndex.getSchema(cls);
		return e.removeIndex(field);
	}

    /**
     * Get the class of a given object.
     */
	@Override
	public long getObjectClass(long oid) {
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw DBLogger.newObjectNotFoundException("OID not found: " + Util.oidToString(oid));
		}
		
		try {
			//TODO use ObjectReader!?!?!
			fileInAP.seekPage(DATA_TYPE.DATA, oie.getPage(), oie.getOffs());
			return new DataDeSerializerNoClass(fileInAP).getClassOid();
		} catch (Exception e) {
			throw DBLogger.newObjectNotFoundException(
					"ERROR reading object: " + Util.oidToString(oid));
		}
	}
	
	@Override
	public int getStats(STATS stats) {
		switch (stats) {
		case IO_DATA_PAGE_READ_CNT:
			return ObjectReader.statsGetReadCount();
		case IO_DATA_PAGE_READ_CNT_UNQ:
			return ObjectReader.statsGetReadCountUnique();
		case IO_PAGE_READ_CNT:
			return file.statsGetReadCount();
		case IO_PAGE_READ_CNT_UNQ:
			return file.statsGetReadCountUnique();
		case IO_PAGE_WRITE_CNT:
			return file.statsGetWriteCount();
		case DB_PAGE_CNT:
			return file.statsGetPageCount();
		case DB_PAGE_CNT_IDX_FSM:
			return freeIndex.debugPageIds().size();
		case DB_PAGE_CNT_IDX_OID:
			return oidIndex.debugPageIds().size();
		case DB_PAGE_CNT_IDX_ATTRIBUTES:
			return schemaIndex.debugPageIdsAttrIdx().size();
		case DB_PAGE_CNT_DATA: {
			PrimLongMapLI<Object> pages = new PrimLongMapLI<Object>();
	        for (SchemaIndexEntry se: schemaIndex.getSchemata()) {
	            PagedPosIndex.ObjectPosIteratorMerger opi = se.getObjectIndexIterator();
	            while (opi.hasNextOPI()) {
	                long pos = opi.nextPos();
	                pages.put(BitTools.getPage(pos), null);
	            }
	        }
	        return pages.size();
		}
		case DB_PAGE_CNT_IDX_POS: {
	        int nPosIndexPages = 0;
	        for (SchemaIndexEntry se: schemaIndex.getSchemata()) {
	            for (int v = 0; v < se.getObjectIndexVersionCount(); v++) {
	                nPosIndexPages += se.getObjectIndexVersion(v).debugPageIds().size();
	            }
	        }
	        return nPosIndexPages;
		}
		case TX_MGR_BUFFERED_TX_CNT:
			return sm.getTxManager().statsGetBufferedTxCount();
		case TX_MGR_BUFFERED_OID_CNT:
			return sm.getTxManager().statsGetBufferedOidCount();
		default:
			throw new IllegalArgumentException("Unknown stat:" + stats);
		}
	}

    @Override
    public String checkDb() {
        final byte ROOT = 1;
        final byte IDX_FSM = 2;
        final byte IDX_OID = 3;
        final byte IDX_POS = 4;
        final byte IDX_ATTR = 5;
        final byte IDX_SCH = 6;
        final byte DATA = 7;
        final byte FREE = 8;
        
        int nObjects = 0;
        int nObjectsByPos = 0;
        int nPosEntries = 0;
        int nPagesFree = 0;
        int nPagesRoot = 0;
        int nPagesData = 0;
        
        int nPages = freeIndex.getPageCount(); 
        
        byte[] pages = new byte[nPages];
        pages[0] = ROOT;
        pages[1] = ROOT;
        pages[2] = ROOT;
        
        //count objects
        Iterator<FilePos> oi = oidIndex.iterator();
        while (oi.hasNext()) {
            FilePos fp = oi.next();
            pages[fp.getPage()] = DATA;
            nObjects++;
        }
        
        Collection<SchemaIndexEntry> sList = schemaIndex.getSchemata();
        int nPosIndexPages = 0;
        for (SchemaIndexEntry se: sList) {
        	for (int v = 0; v < se.getObjectIndexVersionCount(); v++) {
        		PagedPosIndex ppi = se.getObjectIndexVersion(v);
        		LongLongIterator<LongLongIndex.LLEntry> it = ppi.iteratorPositions();
        		while (it.hasNext()) {
        			LongLongIndex.LLEntry lle = it.next();
        			nPosEntries++;
        			long pos = lle.getKey();
        			pages[BitTools.getPage(pos)] = DATA;
        			if (lle.getValue() == 0) {
        				nObjectsByPos++;
        			}
        		}
        		it.close();
        	}
            //pages used by pos-index
            for (int v = 0; v < se.getObjectIndexVersionCount(); v++) {
                List<Integer> pageList = se.getObjectIndexVersion(v).debugPageIds();
                nPosIndexPages += pageList.size();
                for (Integer i: pageList) {
                    if (pages[i] != 0) {
                        System.err.println("Page is double-assigned: " + i);
                    }
                    pages[i] = IDX_POS;
                }
            }
        }

        //pages used by attr-index
        List<Integer> pageList = schemaIndex.debugPageIdsAttrIdx();
        int nAttrIndexPages = pageList.size();
        for (Integer i: pageList) {
            if (pages[i] != 0) {
                System.err.println("Page is double-assigned: " + i);
            }
            pages[i] = IDX_ATTR;
        }
        
        //index pages
        pageList = oidIndex.debugPageIds();
        int nOidPages = pageList.size();
        for (Integer i: pageList) {
            if (pages[i] != 0) {
                System.err.println("Page is double-assigned: " + i);
            }
            pages[i] = IDX_OID;
        }
        
        pageList = freeIndex.debugPageIds();
        int nFsmPages = pageList.size();
        for (Integer i: pageList) {
            if (pages[i] != 0) {
                System.err.println("Page is double-assigned: " + i);
            }
            pages[i] = IDX_FSM;
        }
        

        //free pages
        Iterator<LongLongIndex.LLEntry> fiIter = freeIndex.debugIterator();
        int nPagesFreeDoNotUse = 0;
        while (fiIter.hasNext()) {
            LongLongIndex.LLEntry e = fiIter.next();
            if (pages[(int) e.getKey()] != 0) {
            	if (e.getValue() == 0) {
            		System.err.println("Page is free and assigned at the same time: " + e.getKey());
            	} else {
            		nPagesFreeDoNotUse++;
            	}
            }
            pages[(int) e.getKey()] = FREE;
            nPagesFree++;
        }
        
        //SchemaIndex
        int nSchemaIndexPages = 0;
        for (int pageId: schemaIndex.debugGetPages()) {
        	if (pages[pageId] != 0) {
        		System.err.println("Pages is already assigned: page[" + pageId + "] = " + pages[pageId]);
        	}
        	pages[pageId] = IDX_SCH;
        	nSchemaIndexPages++;
        }
        

        int nPagesFree2 = 0;
        int nPagesUnknown = 0;
        int nIndexPages = 0;
        int nnn=0;
        for (byte b: pages) {
            switch (b) {
            case ROOT: nPagesRoot++; break;
            case DATA: nPagesData++; break;
            case FREE: nPagesFree2++; break;
            case IDX_FSM: 
            case IDX_ATTR:
            case IDX_OID:
            case IDX_POS:
            case IDX_SCH: nIndexPages++; break;
            default: nPagesUnknown++; System.out.print("up=" + nnn + "; ");
            }
            nnn++;
        }
        
        FormattedStringBuilder sb = new FormattedStringBuilder();
        sb.appendln("Objects:                " + nObjects + " / " + nObjectsByPos);
        if (nObjects != nObjectsByPos) {
            sb.appendln("ERROR Object count mismatch for OID index and POS index!");
        }
        sb.appendln("Schemata:               " + sList.size());
        sb.appendln("Pos entries:            " + nPosEntries);
        sb.appendln();
        sb.appendln("OID index pages:        " + nOidPages);
        sb.appendln("FSM index pages:        " + nFsmPages);
        sb.appendln("(FSM-do-not-use pages): " + nPagesFreeDoNotUse);
        sb.appendln("POS index pages:        " + nPosIndexPages);
        sb.appendln("ATTR index pages:       " + nAttrIndexPages);
        sb.appendln("SCH index pages:        " + nSchemaIndexPages);
        sb.appendln("Total index pages:      " + nIndexPages);
        sb.appendln();
        sb.appendln("Free pages:             " + nPagesFree + " / " + nPagesFree2);
        sb.appendln("Data pages:             " + nPagesData);
        sb.appendln("Root pages:             " + nPagesRoot);
        sb.appendln("Index pages:            " + nIndexPages);
        sb.appendln("Unknown pages:          " + nPagesUnknown);
        sb.appendln("Total pages:            " + nPages);
        
        return sb.toString();
    }
}
