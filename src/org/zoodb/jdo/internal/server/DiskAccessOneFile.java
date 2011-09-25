package org.zoodb.jdo.internal.server;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

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
import org.zoodb.jdo.internal.client.CachedObject;
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
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.CloseableIterator;
import org.zoodb.jdo.stuff.DatabaseLogger;

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
 * Current limitations
 * ===================
 * - allow deletion and reuse of pages -> deleted page index? Or via chaining deleted pages?
 *   -> expensive, because page need to be rewritten to chain following page.
 * 
 * 
 * @author Tilmann Zäschke
 *
 */
public class DiskAccessOneFile implements DiskAccess {
	
	public static final int DB_FILE_TYPE_ID = 13031975;
	public static final int DB_FILE_VERSION_MAJ = 1;
	public static final int DB_FILE_VERSION_MIN = 1;
	private static final long ID_FAULTY_PAGE = Long.MIN_VALUE;
	
	private final Node _node;
	private final AbstractCache _cache;
	private final PageAccessFile _raf;
	private final DataDeSerializer _dds;
	
	private final int[] _rootPages = new int[2];
	private int _rootPageID = 0;
	
	private final SchemaIndex _schemaIndex;
	private final PagedOidIndex _oidIndex;
	private final FreeSpaceManager _freeIndex;
	private final PagedObjectAccess _objectWriter;
	private final RootPage rootPage;
	
	
	public DiskAccessOneFile(Node node, AbstractCache cache) {
		_node = node;
		_cache = cache;
		String dbPath = _node.getDbPath();

		
		DatabaseLogger.debugPrintln(1, "Opening DB file: " + dbPath);

		//create DB file
		_freeIndex = new FreeSpaceManager();
		_raf = createPageAccessFile(dbPath, "rw", _freeIndex);

		//read header
		int ii = _raf.readInt();
		if (ii != DB_FILE_TYPE_ID) { 
			throw new JDOFatalDataStoreException("Illegal File ID: " + ii);
		}
		ii = _raf.readInt();
		if (ii != DB_FILE_VERSION_MAJ) { 
			throw new JDOFatalDataStoreException("Illegal major file version: " + ii);
		}
		ii = _raf.readInt();
		if (ii != DB_FILE_VERSION_MIN) { 
			throw new JDOFatalDataStoreException("Illegal minor file version: " + ii);
		}

		int pageSize = _raf.readInt();
		if (pageSize != Config.getFilePageSize()) {
			//TODO actually, in this case would should just close the file and reopen it with the
			//correct page size.
			throw new JDOFatalDataStoreException("Incompatible page size: " + pageSize);
		}
		
		//main directory
		rootPage = new RootPage();
		_rootPages[0] = _raf.readInt();
		_rootPages[1] = _raf.readInt();

		//check root pages
		//we have two root pages. They are used alternatingly.
		long r0 = checkRoot(_rootPages[0]);
		long r1 = checkRoot(_rootPages[1]);
		if (r0 > r1) {
			_rootPageID = 0;
		} else {
			_rootPageID = 1;
		}
		if (r0 == ID_FAULTY_PAGE && r1 == ID_FAULTY_PAGE) {
			String m = "Database is corrupted and cannot be recoverd. Please restore from backup.";
			DatabaseLogger.severe(m);
			throw new JDOFatalDataStoreException(m);
		}

		//readMainPage
		_raf.seekPageForRead(_rootPages[_rootPageID], false);

		//read main directory (page IDs)
		//tx ID
		rootPage.setTxId( _raf.readLong() );
		//User table 
		int userPage = _raf.readInt();
		//OID table
		int oidPage1 = _raf.readInt();
		//schemata
		int schemaPage1 = _raf.readInt();
		//indices
		int indexPage = _raf.readInt();
		//free space index
		int freeSpacePage = _raf.readInt();
		//page count (required for recovery of crashed databases)
		int pageCount = _raf.readInt();
		//last used oid
		long lastUsedOid = _raf.readLong();
		

		//read User data
		_raf.seekPageForRead(userPage, false);
		int userID = _raf.readInt(); //Internal user ID
		User user = Serializer.deSerializeUser(_raf, _node, userID);
		DatabaseLogger.debugPrintln(2, "Found user: " + user.getNameDB());
		_raf.readInt(); //ID of next user, 0=no more users

		//OIDs
		_oidIndex = new PagedOidIndex(_raf.split(), oidPage1, lastUsedOid);

		//dir for schemata
		_schemaIndex = new SchemaIndex(_raf.split(), schemaPage1, false);

		//free space index
		_freeIndex.initBackingIndexLoad(_raf.split(), freeSpacePage, pageCount);
		
		_objectWriter = new PagedObjectAccess(_raf.split(), _oidIndex, _freeIndex);
		
		_dds = new DataDeSerializer(_raf, _cache, _node);
		
		rootPage.set(userPage, oidPage1, schemaPage1, indexPage, freeSpacePage);
	}

	private long checkRoot(int pageId) {
		_raf.seekPageForRead(pageId, false);
		long txID1 = _raf.readLong();
		//skip the data
		for (int i = 0; i < 8; i++) {
			_raf.readInt();
		}
		long txID2 = _raf.readLong();
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
		_rootPageID = (_rootPageID + 1) % 2;
		rootPage.incTxId();
		
		_raf.seekPageForWrite(_rootPages[_rootPageID], false);

		//**********
		// When updating this, also update checkRoot()!
		//**********
		
		//tx ID
		_raf.writeLong(rootPage.getTxId());
		//User table
		_raf.writeInt(userPage);
		//OID table
		_raf.writeInt(oidPage);
		//schemata
		_raf.writeInt(schemaPage);
		//indices
		_raf.writeInt(indexPage);
		//free space index
		_raf.writeInt(freeSpaceIndexPage);
		//page count
		_raf.writeInt(_freeIndex.getPageCount());
		//last used oid
		_raf.writeLong(_oidIndex.getLastUsedOid());
		//tx ID. Writing the tx ID twice should ensure that the data between the two has been
		//written correctly.
		_raf.writeLong(rootPage.getTxId());
	}
	
	/**
	 * @return Null, if no matching schema could be found.
	 */
	@Override
	public ZooClassDef readSchema(String clsName, ZooClassDef defSuper) {
		return _schemaIndex.readSchema(clsName, defSuper, _oidIndex);
	}

	
	/**
	 * @return List of all schemata in the database. These are loaded when the database is opened.
	 */
	@Override
	public Collection<ZooClassDef> readSchemaAll() {
		return _schemaIndex.readSchemaAll(_oidIndex);
	}


	@Override
	public void writeSchema(ZooClassDef sch, boolean isNew, long oid) {
		_schemaIndex.writeSchema(sch, isNew, oid, _oidIndex);
	}
	

	@Override
	public void deleteSchema(ZooClassDef sch) {
		_schemaIndex.deleteSchema(sch, _oidIndex);
	}


	@Override
	public long[] allocateOids(int oidAllocSize) {
		long[] ret = _oidIndex.allocateOids(oidAllocSize);
		return ret;
	}
	
	@Override
	public void deleteObjects(long schemaOid, ArrayList<CachedObject> objects) {
		PagedPosIndex oi = _schemaIndex.getSchema(schemaOid).getObjectIndex();
		for (CachedObject co: objects) {
			long oid = co.getOID();
			long pos = _oidIndex.removeOidNoFail(oid, -1); //value=long with 32=page + 32=offs
			if (pos == -1) {
				throw new JDOObjectNotFoundException("Object not found: " + Util.oidToString(oid));
			}
			
			//update class index and
			//tell the FSM about the free page (if we have one)
			//prevPos.getValue() returns > 0, so the loop is performed at least once.
			do {
				//remove and report to FSM if applicable
                long nextPos = oi.removePosLongAndCheck(pos, _freeIndex);
                //use mark for secondary pages
                nextPos = nextPos | PagedPosIndex.MARK_SECONDARY;
               	pos = nextPos;
			} while (pos != PagedPosIndex.MARK_SECONDARY);
		}
	}
	
	@Override
	public void writeObjects(ZooClassDef clsDef, ArrayList<CachedObject> cachedObjects) {
		if (cachedObjects.isEmpty()) {
			return;
		}
		
		SchemaIndexEntry schema = _schemaIndex.getSchema(clsDef.getOid()); 
		if (schema == null) {
			//TODO catch this a bit earlier in makePeristent() ?!
			throw new JDOFatalDataStoreException("Class has no schema defined: " + 
					clsDef.getClassName());
		}
		PagedPosIndex posIndex = schema.getObjectIndex();

		//start a new page for objects of this class.
		_objectWriter.newPage(posIndex);
		
		DataSerializer dSer = new DataSerializer(_objectWriter, _cache, _node);

		//1st loop: write objects (this also updates the OoiIndex, which carries the objects' 
		//locations
		for (CachedObject co: cachedObjects) {
			long oid = co.getOID();
			PersistenceCapableImpl obj = co.getObject();

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
				_objectWriter.startWriting(oid);
				dSer.writeObject(obj, clsDef, oid);
				_objectWriter.finishObject();
			} catch (Exception e) {
				throw new JDOFatalDataStoreException("Error writing object: " + 
						Util.oidToString(oid), e);
			}
		}
		_objectWriter.finishPage();
		
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
			SchemaIndexEntry schemaTop = _schemaIndex.getSchema(field.getDeclaringType().getOid()); 
			LongLongIndex fieldInd = (LongLongIndex) schemaTop.getIndex(field);
			try {
				Field jField = field.getJavaField();
				if (field.isString()) {
					for (CachedObject co: cachedObjects) {
						long l = BitTools.toSortableLong((String)jField.get(co.obj));
						fieldInd.insertLong(l, co.oid);
					}
				} else {
					switch (field.getPrimitiveType()) {
					case BOOLEAN: 
						for (CachedObject co: cachedObjects) {
							fieldInd.insertLong(jField.getBoolean(co.obj) ? 1 : 0, co.oid);
						}
						break;
					case BYTE: 
						for (CachedObject co: cachedObjects) {
							fieldInd.insertLong(jField.getByte(co.obj), co.oid);
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
						for (CachedObject co: cachedObjects) {
							fieldInd.insertLong(jField.getInt(co.obj), co.oid);
						}
						break;
					case LONG: 
						for (CachedObject co: cachedObjects) {
							fieldInd.insertLong(jField.getLong(co.obj), co.oid);
						}
						break;
					case SHORT: 
						for (CachedObject co: cachedObjects) {
							fieldInd.insertLong(jField.getShort(co.obj), co.oid);
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
	 * Read objects. Format: <nextPage> [<oid> <data>]
	 * This should never be necessary. -> add warning?
	 * -> Only required for queries without index, which is worth a warning anyway.
	 */
	@Override
	public CloseableIterator<PersistenceCapableImpl> readAllObjects(long classOid, 
	        boolean loadFromCache) {
		SchemaIndexEntry se = _schemaIndex.getSchema(classOid);
		if (se == null) {
			throw new JDOUserException("Schema not found for class: " + Util.oidToString(classOid));
		}
		
		PagedPosIndex ind = se.getObjectIndex();
		return new ObjectPosIterator(ind.iteratorObjects(), _cache, _raf.split(), _node, 
		        loadFromCache);
	}
	
	@Override
	public CloseableIterator<PersistenceCapableImpl> readObjectFromIndex(
			ZooFieldDef field, long minValue, long maxValue, boolean loadFromCache) {
		SchemaIndexEntry se = _schemaIndex.getSchema(field.getDeclaringType().getOid());
		LongLongIndex fieldInd = (LongLongIndex) se.getIndex(field);
		AbstractPageIterator<LLEntry> iter = fieldInd.iterator(minValue, maxValue);
		return new ObjectIterator(iter, _cache, this, field, fieldInd, _raf, _node, loadFromCache);
	}	
	
	
	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	@Override
	public PersistenceCapableImpl readObject(long oid) {
		return readObject(_dds, oid);
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
		FilePos oie = _oidIndex.findOid(oid);
		if (oie == null) {
			//throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
			return null;
		}
		
		try {
			return dds.readObject(oie.getPage(), oie.getOffs());
		} catch (Exception e) {
			throw new JDOObjectNotFoundException(
					"ERROR reading object: " + Util.oidToString(oid), e);
		}
	}

	@Override
	public void close() {
		DatabaseLogger.debugPrintln(1, "Closing DB file: " + _node.getDbPath());
		_raf.close();
	}

	@Override
	public void commit() {
		int oidPage = _oidIndex.write();
		int schemaPage1 = _schemaIndex.write();
		int userPage = rootPage.getUserPage(); //not updated currently
		int indexPage = rootPage.getIndexPage(); //TODO remove this?

		//This needs to be written last, because it is updated by other write methods which add
		//new pages to the FSM.
		int freePage = _freeIndex.write();
		
		if (!rootPage.isDirty(userPage, oidPage, schemaPage1, indexPage, freePage)) {
			return;
		}
		rootPage.set(userPage, oidPage, schemaPage1, indexPage, freePage);
		
		// flush the file including all splits 
		_raf.flush(); 
		writeMainPage(userPage, oidPage, schemaPage1, indexPage, freePage);
		//Second flush to update root pages.
		_raf.flush(); 
		
		//tell FSM that new free pages can now be reused.
		_freeIndex.notifyCommit();
	}

	/**
	 * Defines an index and populates it. All objects are put into the cache. This is not 
	 * necessarily useful, but it is a one-off operation. Otherwise we would need a special
	 * purpose implementation of the deserializer, which would have the need for a cache removed.
	 */
	@Override
	public void defineIndex(ZooClassDef cls, ZooFieldDef field, boolean isUnique) {
		SchemaIndexEntry se = _schemaIndex.getSchema(cls.getOid());
		LongLongIndex fieldInd = (LongLongIndex) se.defineIndex(field, isUnique);
		
		//fill index with existing objects
		PagedPosIndex ind = se.getObjectIndex();
		Iterator<LLEntry> iter = ind.iteratorObjects();
        DataDeSerializerNoClass dds = new DataDeSerializerNoClass(_raf);
        if (field.isPrimitiveType()) {
			while (iter.hasNext()) {
				LLEntry oie = iter.next();
				_raf.seekPos(oie.getKey(), true);
				//first read the key, then afterwards the field!
				long key = dds.getAttrAsLong(cls, field);
				fieldInd.insertLong(key, dds.getLastOid());
			}
        } else {
			while (iter.hasNext()) {
				LLEntry oie = iter.next();
				_raf.seekPos(oie.getKey(), true);
				//first read the key, then afterwards the field!
				long key = dds.getAttrAsLongObject(cls, field);
				fieldInd.insertLong(key, dds.getLastOid());
				//TODO handle null values:
				//-ignore them?
				//-use special value?
			}
			//DatabaseLogger.debugPrintln(0, "FIXME defineIndex()");
        }
	}

	@Override
	public boolean removeIndex(ZooClassDef cls, ZooFieldDef field) {
		//TODO return index to FSM
		SchemaIndexEntry e = _schemaIndex.getSchema(cls.getOid());
		return e.removeIndex(field);
	}

	private DataDeSerializerNoClass prepareDeserializer(long oid) {
		FilePos oie = _oidIndex.findOid(oid);
		if (oie == null) {
			throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
		}
		
		try {
			_raf.seekPage(oie.getPage(), oie.getOffs(), true);
			return new DataDeSerializerNoClass(_raf);
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
}
