package org.zoodb.jdo.internal.server;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.Config;
import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.DataDeSerializerNoClass;
import org.zoodb.jdo.internal.DataSerializer;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Serializer;
import org.zoodb.jdo.internal.User;
import org.zoodb.jdo.internal.Util;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.AbstractCache;
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
import org.zoodb.jdo.spi.PersistenceCapableImpl;

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
	public static final int DB_FILE_VERSION_MIN = 1;
	private static final long ID_FAULTY_PAGE = Long.MIN_VALUE;
	
	private final Node node;
	private final AbstractCache cache;
	private final PageAccessFile raf;
	private final DataDeSerializer dds;
	
	private final int[] rootPages = new int[2];
	private int rootPageID = 0;
	
	private final SchemaIndex schemaIndex;
	private final PagedOidIndex oidIndex;
	private final FreeSpaceManager freeIndex;
    private final PagedObjectAccess objectWriter;
	private final RootPage rootPage;
	
	
	public DiskAccessOneFile(Node node, AbstractCache cache) {
		this.node = node;
		this.cache = cache;
		String dbPath = this.node.getDbPath();

		
		DatabaseLogger.debugPrintln(1, "Opening DB file: " + dbPath);

		//create DB file
		freeIndex = new FreeSpaceManager();
		raf = createPageAccessFile(dbPath, "rw", freeIndex);

		//read header
		int ii = raf.readInt();
		if (ii != DB_FILE_TYPE_ID) { 
			throw new JDOFatalDataStoreException("Illegal File ID: " + ii);
		}
		ii = raf.readInt();
		if (ii != DB_FILE_VERSION_MAJ) { 
			throw new JDOFatalDataStoreException("Illegal major file version: " + ii);
		}
		ii = raf.readInt();
		if (ii != DB_FILE_VERSION_MIN) { 
			throw new JDOFatalDataStoreException("Illegal minor file version: " + ii);
		}

		int pageSize = raf.readInt();
		if (pageSize != Config.getFilePageSize()) {
			//TODO actually, in this case would should just close the file and reopen it with the
			//correct page size.
			throw new JDOFatalDataStoreException("Incompatible page size: " + pageSize);
		}
		
		//main directory
		rootPage = new RootPage();
		rootPages[0] = raf.readInt();
		rootPages[1] = raf.readInt();

		//check root pages
		//we have two root pages. They are used alternatingly.
		long r0 = checkRoot(rootPages[0]);
		long r1 = checkRoot(rootPages[1]);
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
		raf.seekPageForRead(rootPages[rootPageID], false);

		//read main directory (page IDs)
		//tx ID
		rootPage.setTxId( raf.readLong() );
		//User table 
		int userPage = raf.readInt();
		//OID table
		int oidPage1 = raf.readInt();
		//schemata
		int schemaPage1 = raf.readInt();
		//indices
		int indexPage = raf.readInt();
		//free space index
		int freeSpacePage = raf.readInt();
		//page count (required for recovery of crashed databases)
		int pageCount = raf.readInt();
		//last used oid
		long lastUsedOid = raf.readLong();
		

		//read User data
		raf.seekPageForRead(userPage, false);
		int userID = raf.readInt(); //Internal user ID
		User user = Serializer.deSerializeUser(raf, this.node, userID);
		DatabaseLogger.debugPrintln(2, "Found user: " + user.getNameDB());
		raf.readInt(); //ID of next user, 0=no more users

		//OIDs
		oidIndex = new PagedOidIndex(raf.split(), oidPage1, lastUsedOid);

		//dir for schemata
		schemaIndex = new SchemaIndex(raf.split(), schemaPage1, false);

		//free space index
		freeIndex.initBackingIndexLoad(raf.split(), freeSpacePage, pageCount);
		
        objectWriter = new PagedObjectAccess(raf.split(), oidIndex, freeIndex);
		
		PagedObjectAccess poa = new PagedObjectAccess(raf, oidIndex, freeIndex);
		dds = new DataDeSerializer(poa, this.cache, this.node);
		
		rootPage.set(userPage, oidPage1, schemaPage1, indexPage, freeSpacePage);
	}

	private long checkRoot(int pageId) {
		raf.seekPageForRead(pageId, false);
		long txID1 = raf.readLong();
		//skip the data
		for (int i = 0; i < 8; i++) {
			raf.readInt();
		}
		long txID2 = raf.readLong();
		if (txID1 == txID2) {
			return txID1;
		}
		DatabaseLogger.severe("Main page is faulty: " + pageId + ". Will recover from previous " +
				"page version.");
		return ID_FAULTY_PAGE;
	}

	private static PageAccessFile createPageAccessFile(String dbPath, String options, 
			FreeSpaceManager fsm) {
		try {
			Class<?> cls = Class.forName(Config.getFileProcessor());
			Constructor<?> con = cls.getConstructor(String.class, String.class, Integer.TYPE, 
						FreeSpaceManager.class);
			PageAccessFile paf = 
				(PageAccessFile) con.newInstance(dbPath, options, Config.getFilePageSize(), fsm);
			return paf;
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("path=" + dbPath, e);
		}
	}
	
	/**
	 * Writes the main page.
	 */
	private void writeMainPage(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage) {
		rootPageID = (rootPageID + 1) % 2;
		rootPage.incTxId();
		
		raf.seekPageForWrite(rootPages[rootPageID], false);

		//**********
		// When updating this, also update checkRoot()!
		//**********
		
		//tx ID
		raf.writeLong(rootPage.getTxId());
		//User table
		raf.writeInt(userPage);
		//OID table
		raf.writeInt(oidPage);
		//schemata
		raf.writeInt(schemaPage);
		//indices
		raf.writeInt(indexPage);
		//free space index
		raf.writeInt(freeSpaceIndexPage);
		//page count
		raf.writeInt(freeIndex.getPageCount());
		//last used oid
		raf.writeLong(oidIndex.getLastUsedOid());
		//tx ID. Writing the tx ID twice should ensure that the data between the two has been
		//written correctly.
		raf.writeLong(rootPage.getTxId());
	}
	
	/**
	 * @return Null, if no matching schema could be found.
	 */
	@Override
	public ZooClassDef readSchema(String clsName, ZooClassDef defSuper) {
		return schemaIndex.readSchema(clsName, defSuper, oidIndex);
	}

	
	/**
	 * @return List of all schemata in the database. These are loaded when the database is opened.
	 */
	@Override
	public Collection<ZooClassDef> readSchemaAll() {
		return schemaIndex.readSchemaAll(oidIndex);
	}


	@Override
	public void writeSchema(ZooClassDef sch, boolean isNew, long oid) {
		schemaIndex.writeSchema(sch, isNew, oid, oidIndex);
	}
	
	@Override
	public void defineSchema(ZooClassDef def) {
		schemaIndex.defineSchema(def);
	}

	@Override
	public void undefineSchema(ZooClassDef sch) {
		schemaIndex.undefineSchema(sch);
	}

	@Override
	public void deleteSchema(ZooClassDef sch) {
		schemaIndex.deleteSchema(sch, oidIndex);
	}

	@Override
	public long[] allocateOids(int oidAllocSize) {
		long[] ret = oidIndex.allocateOids(oidAllocSize);
		return ret;
	}
	
	@Override
	public void deleteObjects(long schemaOid, ArrayList<PersistenceCapableImpl> objects) {
		PagedPosIndex oi = schemaIndex.getSchema(schemaOid).getObjectIndex();
		for (PersistenceCapableImpl co: objects) {
			long oid = co.jdoZooGetOid();
			long pos = oidIndex.removeOidNoFail(oid, -1); //value=long with 32=page + 32=offs
			if (pos == -1) {
				throw new JDOObjectNotFoundException("Object not found: " + Util.oidToString(oid));
			}
			
			//update class index and
			//tell the FSM about the free page (if we have one)
			//prevPos.getValue() returns > 0, so the loop is performed at least once.
			do {
				//remove and report to FSM if applicable
                long nextPos = oi.removePosLongAndCheck(pos, freeIndex);
                //use mark for secondary pages
                nextPos = nextPos | PagedPosIndex.MARK_SECONDARY;
               	pos = nextPos;
			} while (pos != PagedPosIndex.MARK_SECONDARY);
		}
	}
	
	@Override
	public void dropInstances(ZooClassDef def) {
		long schemaOid = def.getOid();
		PagedPosIndex oi = schemaIndex.getSchema(schemaOid).getObjectIndex();
		PagedPosIndex.ObjectPosIterator it = oi.iteratorObjects();
		
        DataDeSerializerNoClass dds = new DataDeSerializerNoClass(raf);
		while (it.hasNextOPI()) {
			long pos = it.nextPos();
			//simply remove all pages
			freeIndex.reportFreePage(BitTools.getPage(pos));
			
			dds.seekPos(pos);
			//first read the key, then afterwards the field!
			long oid = dds.getOid(def);
			oidIndex.removeOidNoFail(oid, -1); //value=long with 32=page + 32=offs
			
			//TODO remove:, instead use clear() below:
			oi.removePosLong(pos);
		}
		//TODO
		//oi.clear();
	}
	
	@Override
	public void writeObjects(ZooClassDef clsDef, ArrayList<PersistenceCapableImpl> cachedObjects) {
		if (cachedObjects.isEmpty()) {
			return;
		}
		
		SchemaIndexEntry schema = schemaIndex.getSchema(clsDef.getOid()); 
		if (schema == null) {
			//TODO catch this a bit earlier in makePeristent() ?!
			throw new JDOFatalDataStoreException("Class has no schema defined: " + 
					clsDef.getClassName());
		}
		PagedPosIndex posIndex = schema.getObjectIndex();

		//start a new page for objects of this class.
		objectWriter.newPage(posIndex, schema.getOID());
		
		DataSerializer dSer = new DataSerializer(objectWriter, cache, node);

		//1st loop: write objects (this also updates the OoiIndex, which carries the objects' 
		//locations
		for (PersistenceCapableImpl obj: cachedObjects) {
			long oid = obj.jdoZooGetOid();

			//TODO this is COW. Currently we rewrite only the new and updated objects. The old
			//version is left were it is; other objects from the old page are not rewritten (unless
			//they changed as well. This also reduces index (location) updates.
			//This speeds up writing, because we write only what is necessary. On the other hand,
			//we may waste a lot of space, because other pages can not be reclaimed 
			//easily -> clean up process? Worst case, the user commits objects individually. Then
			//each object is on a separate page!
			
			//TODO free the page of that object (if there are no other objects on it...
			//-> sort OID by page to see efficiently check whether the last obj was removed???
			//Or... re-write all object from previous page? Even if they haven't changed?

			//TODO rethink the following in perspective of cow...
			//TODO sorting for page: OidIndex is sorted by OID, but SchemaIndex could be sorted by page.
			//Unfortunately, the SchemaIndex doesn't have the page. Should that be reversed? But then
			//SchemaIndex would always be required for reads and writes (especially writes, because it's
			//updated. Better: maintain page/ofs in both indexes?
			//Alternatively: store OID list in the beginning/end of each page. Solves the problem, but
			//... yes, but what would be the problem? Is there any???
			
			//TODO
			//TODO see above, write OIDs into each page!
			//TODO
			
			
			try {
				//update schema index and oid index
				objectWriter.startObject(oid);
				dSer.writeObject(obj, clsDef, oid);
				objectWriter.finishObject();
			} catch (Exception e) {
				throw new JDOFatalDataStoreException("Error writing object: " + 
						Util.oidToString(oid), e);
			}
		}
		
		//2nd loop: update field indices
		//TODO this needs to be done differently. We need a hook in the makeDirty call to store the
		//previous value of the field such that we can remove it efficiently from the index.
		//Or is there another way, maybe by updating an (or the) index?
		//Until then, we just load the object and check whether the index may be out-dated, in which
		//case we remove the entry from the index.
		for (ZooFieldDef field: clsDef.getAllFields()) {
			if (!field.isIndexed()) {
				continue;
			}
			
			//TODO ?
			//LongLongIndex fieldInd = (LongLongIndex) schema.getIndex(field);
			//For now we assume that all sub-classes are indexed as well automatically, so there
			//is only one index which is defined in the top-most class
			SchemaIndexEntry schemaTop = schemaIndex.getSchema(field.getDeclaringType().getOid()); 
			LongLongIndex fieldInd = (LongLongIndex) schemaTop.getIndex(field);
			try {
				Field jField = field.getJavaField();
				if (field.isString()) {
					for (PersistenceCapableImpl co: cachedObjects) {
						long l = BitTools.toSortableLong((String)jField.get(co));
						fieldInd.insertLong(l, co.jdoZooGetOID());
					}
				} else {
					switch (field.getPrimitiveType()) {
					case BOOLEAN: 
						for (PersistenceCapableImpl co: cachedObjects) {
							fieldInd.insertLong(jField.getBoolean(co) ? 1 : 0, co.jdoZooGetOID());
						}
						break;
					case BYTE: 
						for (PersistenceCapableImpl co: cachedObjects) {
							fieldInd.insertLong(jField.getByte(co), co.jdoZooGetOID());
						}
						break;
					case DOUBLE: 
			    		System.out.println("STUB DiskAccessOneFile.writeObjects(DOUBLE)");
			    		//TODO
	//					for (CachedObject co: cachedObjects) {
							//fieldInd.insertLong(jField.getDouble(co.obj), co.oid);
	//					}
						break;
					case FLOAT:
						//TODO
			    		System.out.println("STUB DiskAccessOneFile.writeObjects(FLOAT)");
	//					for (CachedObject co: cachedObjects) {
	//						fieldInd.insertLong(jField.getFloat(co.obj), co.oid);
	//					}
						break;
					case INT: 
						for (PersistenceCapableImpl co: cachedObjects) {
							fieldInd.insertLong(jField.getInt(co), co.jdoZooGetOID());
						}
						break;
					case LONG: 
						for (PersistenceCapableImpl co: cachedObjects) {
							fieldInd.insertLong(jField.getLong(co), co.jdoZooGetOID());
						}
						break;
					case SHORT: 
						for (PersistenceCapableImpl co: cachedObjects) {
							fieldInd.insertLong(jField.getShort(co), co.jdoZooGetOID());
						}
						break;
						
					default:
						throw new IllegalArgumentException("type = " + field.getPrimitiveType());
					}
				}
			} catch (SecurityException e) {
				throw new JDOFatalDataStoreException(
						"Error accessing field: " + field.getName(), e);
			} catch (IllegalArgumentException e) {
				throw new JDOFatalDataStoreException(
						"Error accessing field: " + field.getName(), e);
			} catch (IllegalAccessException e) {
				throw new JDOFatalDataStoreException(
						"Error accessing field: " + field.getName(), e);
			}
		}
	}

	/**
	 * Read objects.
	 * This should never be necessary. -> add warning?
	 * -> Only required for queries without index, which is worth a warning anyway.
	 */
	@Override
	public CloseableIterator<PersistenceCapableImpl> readAllObjects(long classOid, 
	        boolean loadFromCache) {
		SchemaIndexEntry se = schemaIndex.getSchema(classOid);
		if (se == null) {
			throw new JDOUserException("Schema not found for class: " + Util.oidToString(classOid));
		}
		
		PagedPosIndex ind = se.getObjectIndex();
		return new ObjectPosIterator(ind.iteratorObjects(), cache, objectWriter, node, 
		        loadFromCache);
	}
	
	@Override
	public CloseableIterator<PersistenceCapableImpl> readObjectFromIndex(
			ZooFieldDef field, long minValue, long maxValue, boolean loadFromCache) {
		SchemaIndexEntry se = schemaIndex.getSchema(field.getDeclaringType().getOid());
		LongLongIndex fieldInd = (LongLongIndex) se.getIndex(field);
		AbstractPageIterator<LLEntry> iter = fieldInd.iterator(minValue, maxValue);
		return new ObjectIterator(iter, cache, this, field, fieldInd, objectWriter, node, loadFromCache);
	}	
	
	
	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	@Override
	public PersistenceCapableImpl readObject(long oid) {
		return readObject(dds, oid);
	}

	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	@Override
	public void readObject(PersistenceCapableImpl pc) {
		long oid = pc.jdoZooGetOid();
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
		}
		
		try {
			dds.readObject(pc, oie.getPage(), oie.getOffs());
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
	public PersistenceCapableImpl readObject(DataDeSerializer dds, long oid) {
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			//throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
			return null;
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
		raf.close();
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
		
		if (!rootPage.isDirty(userPage, oidPage, schemaPage1, indexPage, freePage)) {
			return;
		}
		rootPage.set(userPage, oidPage, schemaPage1, indexPage, freePage);
		
		// flush the file including all splits 
		raf.flush(); 
		writeMainPage(userPage, oidPage, schemaPage1, indexPage, freePage);
		//Second flush to update root pages.
		raf.flush(); 
		
		//tell FSM that new free pages can now be reused.
		freeIndex.notifyCommit();
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
        DataDeSerializerNoClass dds = new DataDeSerializerNoClass(raf);
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
				long key = dds.getAttrAsLongObject(cls, field);
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
		//TODO return index to FSM
		SchemaIndexEntry e = schemaIndex.getSchema(cls.getOid());
		return e.removeIndex(field);
	}

	private DataDeSerializerNoClass prepareDeserializer(long oid) {
		FilePos oie = oidIndex.findOid(oid);
		if (oie == null) {
			throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
		}
		
		try {
			raf.seekPage(oie.getPage(), oie.getOffs(), true);
			return new DataDeSerializerNoClass(raf);
		} catch (Exception e) {
			throw new JDOObjectNotFoundException("ERROR reading object: " + Util.oidToString(oid));
		}
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
	public int statsPageWriteCount() {
		return raf.statsGetWriteCount();
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
        int[] nObjectsA = null;
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
        nObjectsA = new int[sList.size()]; 
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
