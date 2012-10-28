/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.ZooConfig;
import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.DataDeSerializerNoClass;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.AbstractPageIterator;
import org.zoodb.jdo.internal.server.index.AbstractPagedIndex.LongLongIndex;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.ObjectIterator;
import org.zoodb.jdo.internal.server.index.ObjectPosIterator;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.PagedOidIndex.FilePos;
import org.zoodb.jdo.internal.server.index.PagedPosIndex;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.server.index.SchemaIndex;
import org.zoodb.jdo.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.DatabaseLogger;
import org.zoodb.jdo.internal.util.FormattedStringBuilder;
import org.zoodb.jdo.internal.util.PoolDDS;
import org.zoodb.jdo.internal.util.Util;

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
 * @author Tilmann Zäschke
 */
public class DiskAccessOneFile implements DiskAccess {
	
	public static final int DB_FILE_TYPE_ID = 13031975;
	public static final int DB_FILE_VERSION_MAJ = 1;
	public static final int DB_FILE_VERSION_MIN = 3;
	private static final long ID_FAULTY_PAGE = Long.MIN_VALUE;
	
	private final Node node;
	private final AbstractCache cache;
	private final StorageChannel file;
	private final StorageChannelInput fileInAP;
	private final StorageChannelOutput fileOut;
	private final PoolDDS ddsPool;
	
	private final int[] rootPages = new int[2];
	private int rootPageID = 0;
	
	private final SchemaIndex schemaIndex;
	private final PagedOidIndex oidIndex;
	private final FreeSpaceManager freeIndex;
    private final ObjectReader objectReader;
	private final RootPage rootPage;
	
	
	public DiskAccessOneFile(Node node, AbstractCache cache) {
		this.node = node;
		this.cache = cache;
		String dbPath = this.node.getDbPath();

		
		DatabaseLogger.debugPrintln(1, "Opening DB file: " + dbPath);

		//create DB file
		freeIndex = new FreeSpaceManager();
		file = createPageAccessFile(dbPath, "rw", freeIndex);
		StorageChannelInput in = file.getReader(false);
		in.seekPageForRead(1);
		in.seekPageForRead(0);
		
		//read header
		int ii = in.readInt();
		if (ii != DB_FILE_TYPE_ID) { 
			throw new JDOFatalDataStoreException("Illegal File ID: " + ii);
		}
		ii = in.readInt();
		if (ii != DB_FILE_VERSION_MAJ) { 
			throw new JDOFatalDataStoreException("Illegal major file version: " + ii);
		}
		ii = in.readInt();
		if (ii != DB_FILE_VERSION_MIN) { 
			throw new JDOFatalDataStoreException("Illegal minor file version: " + ii);
		}

		int pageSize = in.readInt();
		if (pageSize != ZooConfig.getFilePageSize()) {
			//TODO actually, in this case would should just close the file and reopen it with the
			//correct page size.
			throw new JDOFatalDataStoreException("Incompatible page size: " + pageSize);
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
			DatabaseLogger.severe(m);
			throw new JDOFatalDataStoreException(m);
		}

		//readMainPage
		in.seekPageForRead(rootPages[rootPageID]);

		//read main directory (page IDs)
		//tx ID
		rootPage.setTxId( in.readLong() );
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
		//last used oid
		long lastUsedOid = in.readLong();
		
		//OIDs
		oidIndex = new PagedOidIndex(file, oidPage1, lastUsedOid);

		//dir for schemata
		schemaIndex = new SchemaIndex(file, schemaPage1, false);

		//free space index
		freeIndex.initBackingIndexLoad(file, freeSpacePage, pageCount);
		
        objectReader = new ObjectReader(file);
		
		ddsPool = new PoolDDS(file, this.cache, this.node);
		
		rootPage.set(userPage, oidPage1, schemaPage1, indexPage, freeSpacePage, pageCount);

		fileInAP = file.getReader(true);
		fileOut = file.getWriter(false);
	}

	private long checkRoot(StorageChannelInput in, int pageId) {
		in.seekPageForRead(pageId);
		long txID1 = in.readLong();
		//skip the data
		for (int i = 0; i < 8; i++) {
			in.readInt();
		}
		long txID2 = in.readLong();
		if (txID1 == txID2) {
			return txID1;
		}
		DatabaseLogger.severe("Main page is faulty: " + pageId + ". Will recover from previous " +
				"page version.");
		return ID_FAULTY_PAGE;
	}

	private static StorageChannel createPageAccessFile(String dbPath, String options, 
			FreeSpaceManager fsm) {
		try {
			Class<?> cls = Class.forName(ZooConfig.getFileProcessor());
			Constructor<?> con = cls.getConstructor(String.class, String.class, Integer.TYPE, 
					FreeSpaceManager.class);
			StorageChannel paf = 
				(StorageChannel) con.newInstance(dbPath, options, ZooConfig.getFilePageSize(), fsm);
			return paf;
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("path=" + dbPath, e);
		}
	}
	
	/**
	 * Writes the main page.
	 * @param pageCount 
	 */
	private void writeMainPage(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage, int pageCount, StorageChannelOutput out) {
		rootPageID = (rootPageID + 1) % 2;
		rootPage.incTxId();
		
		out.seekPageForWrite(rootPages[rootPageID]);

		//**********
		// When updating this, also update checkRoot()!
		//**********
		
		//tx ID
		out.writeLong(rootPage.getTxId());
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
		out.writeLong(oidIndex.getLastUsedOid());
		//tx ID. Writing the tx ID twice should ensure that the data between the two has been
		//written correctly.
		out.writeLong(rootPage.getTxId());
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
		Collection<ZooClassDef> all = schemaIndex.readSchemaAll(this);
		if (all.isEmpty()) {
			//new database, need to initialize!
			
			//This is the root schema
			ZooClassDef zpcDef = ZooClassDef.bootstrapZooPCImpl(); 
			ZooClassDef meta = ZooClassDef.bootstrapZooClassDef(); 
			zpcDef.associateFields();
			meta.associateFields();
			meta.associateJavaTypes();
			schemaIndex.defineSchema(zpcDef);
			schemaIndex.defineSchema(meta);

			all = new ArrayList<ZooClassDef>();
			all.add(zpcDef);
			all.add(meta);
		}
		return all;
	}


	@Override
	public void defineSchema(ZooClassDef def) {
		schemaIndex.defineSchema(def);
	}

	@Override
	public void undefineSchema(ZooClassDef def) {
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
	public void dropInstances(ZooClassDef def) {
		long schemaOid = def.getOid();
		PagedPosIndex oi = schemaIndex.getSchema(schemaOid).getObjectIndex();
		PagedPosIndex.ObjectPosIterator it = oi.iteratorObjects();
		
		//clean oid index
		DataDeSerializerNoClass dds = new DataDeSerializerNoClass(fileInAP);
		while (it.hasNextOPI()) {
			long pos = it.nextPos();
			//simply remove all pages
			freeIndex.reportFreePage(BitTools.getPage(pos));
			
			dds.seekPos(pos);
			//first read the key, then afterwards the field!
			long oid = dds.getOid(def);
			oidIndex.removeOidNoFail(oid, -1); //value=long with 32=page + 32=offs
		}
		it.close();
		
		//clean field indices
		for (AbstractPagedIndex ind: schemaIndex.getSchema(schemaOid).getIndices()) {
			ind.clear();
		}
		
		//clean pos index
		oi.clear();
	}
	
	@Override
	public SchemaIndexEntry getSchemaIE(long oid) {
	    return schemaIndex.getSchema(oid);
	}
	
	@Override
	public PagedOidIndex getOidIndex() {
	    return oidIndex;
	}
	
	@Override
	public ObjectWriter getWriter(long clsOid) {
	    return new ObjectWriter(file, oidIndex, clsOid);
	}
	
	/**
	 * Read objects.
	 * This should never be necessary. -> add warning?
	 * -> Only required for queries without index, which is worth a warning anyway.
	 */
	@Override
	public CloseableIterator<ZooPCImpl> readAllObjects(long classOid, 
	        boolean loadFromCache) {
		SchemaIndexEntry se = schemaIndex.getSchema(classOid);
		if (se == null) {
			throw new JDOUserException("Schema not found for class: " + Util.oidToString(classOid));
		}
		
		PagedPosIndex ind = se.getObjectIndex();
		return new ObjectPosIterator(ind.iteratorObjects(), cache, objectReader, node, 
		        loadFromCache);
	}
	
	/**
	 * WARNING: float/double values need to be converted with BitTools before used on indices. 
	 */
	@Override
	public CloseableIterator<ZooPCImpl> readObjectFromIndex(
			ZooFieldDef field, long minValue, long maxValue, boolean loadFromCache) {
		SchemaIndexEntry se = schemaIndex.getSchema(field.getDeclaringType().getOid());
		LongLongIndex fieldInd = (LongLongIndex) se.getIndex(field);
		AbstractPageIterator<LLEntry> iter = fieldInd.iterator(minValue, maxValue);
		return new ObjectIterator(iter, cache, this, objectReader, node, loadFromCache);
	}	
	
	
	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	@Override
	public ZooPCImpl readObject(long oid) {
	    final DataDeSerializer dds = ddsPool.get();
		final ZooPCImpl pci = readObject(dds, oid);
		ddsPool.offer(dds);
		return pci;
	}

	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	@Override
	public void readObject(ZooPCImpl pc) {
		long oid = pc.jdoZooGetOid();
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
		}
		
		try {
	        final DataDeSerializer dds = ddsPool.get();
            dds.readObject(pc, oie.getPage(), oie.getOffs());
	        ddsPool.offer(dds);
		} catch (Exception e) {
			throw new JDOObjectNotFoundException(
					"ERROR reading object: " + Util.oidToString(oid), e);
		}
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
	public ZooPCImpl readObject(DataDeSerializer dds, long oid) {
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
		}
		
		try {
			return dds.readObject(oie.getPage(), oie.getOffs(), false);
		} catch (Exception e) {
			throw new JDOObjectNotFoundException(
					"ERROR reading object: " + Util.oidToString(oid), e);
		}
	}

	@Override
	public void close() {
		DatabaseLogger.debugPrintln(1, "Closing DB file: " + node.getDbPath());
		file.close();
	}

	@Override
	public void commit() {
		int oidPage = oidIndex.write();
		int schemaPage1 = schemaIndex.write();
		int userPage = rootPage.getUserPage(); //not updated currently
		int indexPage = rootPage.getIndexPage(); //TODO remove this?

		//This needs to be written last, because it is updated by other write methods which add
		//new pages to the FSM.
		int freePage = freeIndex.write();
		int pageCount = freeIndex.getPageCount();
		
		if (!rootPage.isDirty(userPage, oidPage, schemaPage1, indexPage, freePage)) {
			return;
		}
		rootPage.set(userPage, oidPage, schemaPage1, indexPage, freePage, pageCount);
		
		// flush the file including all splits 
		file.flush(); 
		writeMainPage(userPage, oidPage, schemaPage1, indexPage, freePage, pageCount, fileOut);
		//Second flush to update root pages.
		file.flush(); 
		
		//tell FSM that new free pages can now be reused.
		freeIndex.notifyCommit();
		
		//refresh pos-index iterators, if any exist.
		//TODO not necessary at the moment..., all tests (e.g. Test_62) pass anyway.
		//refresh() is performed through the session object.
		//schemaIndex.refreshIterators();
	}

	/**
	 * This can be called after a failed commit (or JDO flush()). In case the root pages have not 
	 * been rewritten, this method will revert existing changes.
	 */
	@Override
	public void revert() {
		//Empty file buffers. For now we just flush them.
		file.flush(); //TODO revert for file???
		//revert
		schemaIndex.revert(rootPage.getSchemIndexPage());
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
	public void defineIndex(ZooClassDef cls, ZooFieldDef field, boolean isUnique) {
		SchemaIndexEntry se = schemaIndex.getSchema(cls.getOid());
		LongLongIndex fieldInd = (LongLongIndex) se.defineIndex(field, isUnique);
		
		//fill index with existing objects
		PagedPosIndex ind = se.getObjectIndex();
		PagedPosIndex.ObjectPosIterator iter = ind.iteratorObjects();
        DataDeSerializerNoClass dds = new DataDeSerializerNoClass(fileInAP);
        if (field.isPrimitiveType()) {
			while (iter.hasNext()) {
				long pos = iter.nextPos();
				dds.seekPos(pos);
				//first read the key, then afterwards the field!
				long key = dds.getAttrAsLong(cls, field);
				fieldInd.insertLong(key, dds.getLastOid());
			}
        } else {
			while (iter.hasNext()) {
				long pos = iter.nextPos();
				dds.seekPos(pos);
				//first read the key, then afterwards the field!
				long key = dds.getAttrAsLongObjectNotNull(cls, field);
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
		SchemaIndexEntry e = schemaIndex.getSchema(cls.getOid());
		return e.removeIndex(field);
	}

	private DataDeSerializerNoClass prepareDeserializer(long oid) {
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
		}
		
		try {
			fileInAP.seekPage(oie.getPage(), oie.getOffs());
			return new DataDeSerializerNoClass(fileInAP);
		} catch (Exception e) {
			throw new JDOObjectNotFoundException("ERROR reading object: " + Util.oidToString(oid));
		}
	}
	
    /**
     * Read the class of a given object.
     */
	@Override
	public ZooClassDef readObjectClass(long oid) {
		long clsOid = prepareDeserializer(oid).getClassOid();
		return schemaIndex.getSchema(clsOid).getClassDef();
	}

	@Override
	public byte readAttrByte(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrByte(schemaDef, attrHandle);
	}
	
	@Override
	public long readAttrLong(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrLong(schemaDef, attrHandle);
	}
	
	@Override
	public int readAttrInt(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrInt(schemaDef, attrHandle);
	}
	
	@Override
	public char readAttrChar(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrChar(schemaDef, attrHandle);
	}
	
	@Override
	public short readAttrShort(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrShort(schemaDef, attrHandle);
	}
	
	@Override
	public float readAttrFloat(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrFloat(schemaDef, attrHandle);
	}
	
	@Override
	public double readAttrDouble(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrDouble(schemaDef, attrHandle);
	}
	
	@Override
	public boolean readAttrBool(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrBool(schemaDef, attrHandle);
	}
	
	@Override
	public String readAttrString(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		throw new UnsupportedOperationException();
		//return prepareDeserializer(oid).getAttrString(schemaDef, attrHandle);
	}
	
	@Override
	public Date readAttrDate(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		throw new UnsupportedOperationException();
		//return prepareDeserializer(oid).getAttrDate(schemaDef, attrHandle);
	}

	@Override
	public long readAttrRefOid(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return prepareDeserializer(oid).getAttrRefOid(schemaDef, attrHandle);
	}

	@Override
	public int statsPageReadCount() {
		return file.statsGetReadCount();
	}

	@Override
	public int statsPageWriteCount() {
		return file.statsGetWriteCount();
	}

    @Override
    public String checkDb() {
        final byte ROOT = 1;
        final byte IDX_FSM = 2;
        final byte IDX_OID = 3;
        final byte IDX_POS = 4;
        final byte IDX_ATTR = 5;
        final byte DATA = 6;
        final byte FREE = 7;
        
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
            PagedPosIndex.ObjectPosIterator opi = se.getObjectIndex().iteratorObjects();
            while (opi.hasNext()) {
                nPosEntries++;
                long pos = opi.nextPos();
                pages[BitTools.getPage(pos)] = DATA;
                nObjectsByPos++;
            }
            //pages used by pos-index
            List<Integer> pageList = se.getObjectIndex().debugPageIds();
            nPosIndexPages += pageList.size();
            for (Integer i: pageList) {
                if (pages[i] != 0) {
                    System.err.println("Page is double-assigned: " + i);
                }
                pages[i] = IDX_POS;
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
        Iterator<LLEntry> fiIter = freeIndex.debugIterator();
        int nPagesFreeDoNotUse = 0;
        while (fiIter.hasNext()) {
            LLEntry e = fiIter.next();
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
        
        int nPagesFree2 = 0;
        int nPagesUnknown = 0;
        int nIndexPages = 0;
        for (byte b: pages) {
            switch (b) {
            case ROOT: nPagesRoot++; break;
            case DATA: nPagesData++; break;
            case FREE: nPagesFree2++; break;
            case IDX_FSM: 
            case IDX_ATTR:
            case IDX_OID:
            case IDX_POS: nIndexPages++; break;
            default: nPagesUnknown++;
            }
        }
        
        FormattedStringBuilder sb = new FormattedStringBuilder();
        sb.appendln("Objects: " + nObjects + " / " + nObjectsByPos);
        if (nObjects != nObjectsByPos + sList.size()) {
            sb.appendln("ERROR Object count mismatch for OID index and POS index!");
        }
        sb.appendln("Schemata: " + sList.size());
        sb.appendln("Pos entries: " + nPosEntries);
        sb.appendln();
        sb.appendln("OID index pages: " + nOidPages);
        sb.appendln("FSM index pages: " + nFsmPages);
        sb.appendln("(FSM-do-not-use pages): " + nPagesFreeDoNotUse);
        sb.appendln("POS index pages: " + nPosIndexPages);
        sb.appendln("ATTR index pages: " + nAttrIndexPages);
        sb.appendln("Total index pages: " + nIndexPages);
        sb.appendln();
        sb.appendln("Free pages: " + nPagesFree + " / " + nPagesFree2);
        sb.appendln("Data pages: " + nPagesData);
        sb.appendln("Root pages: " + nPagesRoot);
        sb.appendln("Index pages: " + nIndexPages);
        sb.appendln("Unknown pages: " + nPagesUnknown);
        sb.appendln("Total pages: " + nPages);
        
        return sb.toString();
    }
}
