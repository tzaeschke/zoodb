package org.zoodb.jdo.internal.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.DataSerializer;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Serializer;
import org.zoodb.jdo.internal.User;
import org.zoodb.jdo.internal.Util;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.PagedOidIndex.FilePos;
import org.zoodb.jdo.internal.server.index.PagedPosIndex;
import org.zoodb.jdo.internal.server.index.SchemaIndex;
import org.zoodb.jdo.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
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
 *   required objects, the other can be stored in a cache of byte[]!!!!
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
	
	public static final int PAGE_SIZE = 1024*1;
	
	public static final int PAGE_TYPE_OIDS = 10;
	public static final int PAGE_TYPE_SCHEMA = 11;
	public static final int PAGE_TYPE_DATA = 12;
	public static final int PAGE_TYPE_INDEX = 13;
	
	
	private final Node _node;
	private final PageAccessFile _raf;
	
	private int _rootPage1;
	private int _rootPage2;
	
	private int _userPage1;
	private int _indexPage1;
		
	private final SchemaIndex _schemaIndex;
	private final PagedOidIndex _oidIndex;
	private final PagedObjectAccess _objectWriter;
	
	public DiskAccessOneFile(Node node) {
		_node = node;
		String dbName = _node.getDbPath() + File.separator + "zoo.db";

		
		DatabaseLogger.debugPrintln(1,"Opening DB file: " + dbName);

		File dbDir = new File(_node.getDbPath());
		if (!dbDir.exists()) {
			throw new JDOUserException(
					"ZOO: DB folder does not exist: " + dbDir);
		}

		//create files
		try {
			//DB file
			File dbFile = new File(dbName);
			if (!dbFile.exists()) {
				throw new JDOUserException(
						"ZOO: DB file does not exist: " + dbFile);
			}
			//_raf = new PageAccessFile_MappedBB(dbFile, "rw");
			_raf = new PageAccessFile_BB(dbFile, "rw");

			//read header
			int ii =_raf.readInt();
			if (ii != DB_FILE_TYPE_ID) { 
				throw new JDOFatalDataStoreException("Illegal File ID: " + ii);
			}
			ii =_raf.readInt();
			if (ii != DB_FILE_VERSION_MAJ) { 
				throw new JDOFatalDataStoreException("Illegal major file version: " + ii);
			}
			ii =_raf.readInt();
			if (ii != DB_FILE_VERSION_MIN) { 
				throw new JDOFatalDataStoreException("Illegal minor file version: " + ii);
			}
			
			//main directory
			_rootPage1 =_raf.readInt();
			_rootPage2 =_raf.readInt();

			
			//readMainPage
			_raf.seekPage(_rootPage1, false);

			//write main directory (page IDs)
			//User table 
			_userPage1 =_raf.readInt();
			//OID table
			int oidPage1 =_raf.readInt();
			//schemata
			int schemaPage1 =_raf.readInt();
			//indices
			_indexPage1 =_raf.readInt();


			//read User data
			_raf.seekPage(_userPage1, false);
			int userID =_raf.readInt(); //Interal user ID
            User user = Serializer.deSerializeUser(_raf, _node, userID);
			DatabaseLogger.debugPrintln(2, "Found user: " + user.getNameDB());
			_raf.readInt(); //ID of next user, 0=no more users

			//OIDs
			_oidIndex = new PagedOidIndex(_raf, oidPage1);

			//dir for schemata
			_schemaIndex = new SchemaIndex(_raf, schemaPage1, false);

			_objectWriter = new PagedObjectAccess(_raf);

		} catch (IOException e) {
			throw new JDOUserException("ERROR While creating database: " + dbName, e);
		}
	}

	private void writeMainPage(int userPage, int oidPage, int schemaPage, int indexPage) {
		_raf.seekPage(_rootPage1, false);
		
		//User table
		_raf.writeInt(userPage);
		//OID table
		_raf.writeInt(oidPage);
		//schemata
		_raf.writeInt(schemaPage);
		//indices
		_raf.writeInt(indexPage);
	}
	
	/**
	 * @return Null, if no matching schema could be found.
	 */
	@Override
	public ZooClassDef readSchema(String clsName, ZooClassDef defSuper) {
		SchemaIndexEntry e = _schemaIndex.getSchema(clsName);
		if (e == null) {
			return null; //no matching schema found 
		}
		_raf.seekPage(e.getPage(), e.getOffset(), true);
		ZooClassDef def = Serializer.deSerializeSchema(_node, _raf);
		def.setSuperDef(defSuper);
		System.out.println("Do we need this method still?????");
		return def;
	}
	
	/**
	 * @return List of all schemata in the database. These are loaded when the database is opened.
	 */
	@Override
	public Collection<ZooClassDef> readSchemaAll() {
		Map<Long, ZooClassDef> ret = new HashMap<Long, ZooClassDef>();
		for (SchemaIndexEntry se: _schemaIndex.getSchemata()) {
			_raf.seekPage(se.getPage(), se.getOffset(), true);
			ZooClassDef def = Serializer.deSerializeSchema(_node, _raf);
			ret.put( def.getOid(), def );
		}
		// assign super classes
		for (ZooClassDef def: ret.values()) {
			if (def.getSuperOID() != 0) {
				def.setSuperDef( ret.get(def.getSuperOID()) );
			}
		}
		return ret.values();
	}
	
	@Override
	public void writeSchema(ZooClassDef sch, boolean isNew, long oid) {
		String clsName = sch.getClassName();
		SchemaIndexEntry theSchema = _schemaIndex.getSchema(clsName);
		
        if (!isNew) {
            throw new UnsupportedOperationException("Schema evolution not supported.");
            //TODO rewrite all schemata on page.
            //TODO support schema evolution (what other changes can there be???)
        }

        if (isNew && theSchema != null) {
			throw new JDOUserException("Schema already defined: " +	clsName);
		}
		if (!isNew && theSchema == null) {
			throw new JDOUserException("Schema not found: " + clsName);
		}

		//allocate page
		//TODO write all schemata one page (or series of pages)? 
		int schPage = _raf.allocateAndSeek(true);
		Serializer.serializeSchema(_node, sch, oid, _raf);

		//Store OID in index
		if (isNew) {
            int schOffs = 0;
            theSchema = new SchemaIndexEntry(clsName, schPage, schOffs, _raf, oid);
            _schemaIndex.add(theSchema);
			_oidIndex.addOid(oid, schPage, schOffs);
			//TODO add to schema index of Schema class?
			//     -> bootstrap schema classes: CLASS, FIELD (,TYPE)
			System.err.println("XXXX Create schema entry for schemata! -> ObjIndex!");
		}
	}
	

	public void deleteSchema(ZooClassDef sch) {
		//TODO more efficient, do not delete schema (rewriting the index), only flag it as deleted??

		//TODO first delete subclasses
		System.out.println("STUB delete subdata pages.");
		
		String cName = sch.getClassName();
		SchemaIndexEntry entry = _schemaIndex.deleteSchema(cName);
		if (entry == null) {
			throw new JDOUserException("Schema not found: " + cName);
		}

		//update OIDs
		_oidIndex.removeOid(sch.getOid());
		System.err.println("XXXX Create schema entry for schemata! -> ObjIndex!");

		//delete all associated data
//		int dataPage = entry._dataPage;
		System.out.println("STUB delete data pages.");
//		try {
//			while (dataPage != 0) {
//				_raf.seekPage(dataPage);
//				//dataPage = _raf.readInt();
//				//TODO
//				//store oid/len/data OR store list of OIDS in the beginning of the page?
//				//for now: search OIDS for matching instances?
//				//TODO
//				//remove serialized schema from DB and clean up the page it was located on
//				//->  List<Schema> list;
//				//->  list.addAll(schema on page, except the von to delete)
//				//->  rewrite page
//			}
//		} catch (IOException e) {
//			throw new JDOFatalDataStoreException("Error deleting instances: " + cName, e);
//		}
	}

	public long[] allocateOids(int oidAllocSize) {
		long[] ret = _oidIndex.allocateOids(oidAllocSize);
		return ret;
	}
	
	public void deleteObject(Object obj, long oid) {
		FilePos pos = _oidIndex.findOid(oid);
		if (pos == null) {
			throw new JDOObjectNotFoundException("Object not found: " + Util.oidToString(oid));
		}
		
		_oidIndex.removeOid(oid);
		
		//update class index
		PagedPosIndex oi = _schemaIndex.getSchema(obj.getClass().getName()).getObjectIndex();
		oi.removePos(pos);
		
		//TODO
		//System.out.println("STUB delete object from data page: " + Util.oidToString(oid));
	}

	
	@Override
	public void writeObjects(ZooClassDef clsDef, List<CachedObject> cachedObjects, 
			AbstractCache cache) {
		if (cachedObjects.isEmpty()) {
			return;
		}
		
		//start a new page for objects of this class.
		_objectWriter.newPage();
		
		SchemaIndexEntry schema = _schemaIndex.getSchema(clsDef.getClassName()); 
		if (schema == null) {
			//TODO catch this a bit earlier in makePeristent() ?!
			throw new JDOFatalDataStoreException("Class has no schema defined: " + 
					clsDef.getClassName());
		}
		PagedPosIndex posIndex = schema.getObjectIndex();
		
		//first loop: update schema index (may not be fully loaded. TODO find a better solution?
		for (CachedObject co: cachedObjects) {
			boolean isNew = co.isNew();
			long oid = co.getOID();
			
			FilePos oie = _oidIndex.findOid(oid);
			
			if (!isNew && oie == null) {
				throw new JDOFatalDataStoreException("Object not found: " + Util.oidToString(oid));
			} else if (isNew && oie != null) {
				throw new JDOFatalDataStoreException("Object already exists: " + Util.oidToString(oid));
			}
		}

		//2nd loop: write objects (this also updates the OoiIndex, which carries the objects' 
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
				_objectWriter.startWriting(oid);
				DataSerializer dSer = new DataSerializer(_objectWriter, cache, _node);
				dSer.writeObject(obj, clsDef);
				_objectWriter.stopWriting();
			} catch (Exception e) {
				throw new JDOFatalDataStoreException("Error writing object: " + 
						Util.oidToString(oid), e);
			}
			
		}
		_objectWriter.finishChunk(posIndex, _oidIndex);
	}

	/**
	 * Read objects. Format: <nextPage> [<oid> <data>]
	 */
	//TODO return iterator?
	//TODO this should never be necessary. -> add warning
	//     -> Only required for queries without index, which is worth a warning anyway.
	public List<PersistenceCapableImpl> readAllObjects(String className, AbstractCache cache) {
		SchemaIndexEntry se = _schemaIndex.getSchema(className);
		if (se == null) {
			throw new JDOUserException("Schema not found for class: " + className);
		}
		
		List<PersistenceCapableImpl> ret = new ArrayList<PersistenceCapableImpl>();

		PagedPosIndex ind = se.getObjectIndex();
		Iterator<FilePos> iter = ind.posIterator();
		try {
			while (iter.hasNext()) {
                FilePos oie = iter.next();
		        DataDeSerializer dds = new DataDeSerializer(_raf, cache, _node);
				_raf.seekPage(oie.getPage(), oie.getOffs(), true);
				ret.add( dds.readObject() );
			}
			return ret;
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("Error reading objects.", e);
		}
	}
	
	//TODO return iterator?
	public List<PersistenceCapableImpl> readObjects(long[] oids, AbstractCache cache) {
		//TODO optimize to read pages only once, and to read them in order )how would that work??)
		//-> read them all into the cache
		List<PersistenceCapableImpl> ret = new ArrayList<PersistenceCapableImpl>();
		for (long oid: oids) {
			ret.add( readObject(cache, oid) );
		}
		return ret;
	}
	
	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	public PersistenceCapableImpl readObject(AbstractCache cache, long oid) {
		FilePos oie = _oidIndex.findOid(oid);
		if (oie == null) {
			//throw new JDOObjectNotFoundException("ERROR OID not found: " + Util.oidToString(oid));
			return null;
		}
		
		try {
			_raf.seekPage(oie.getPage(), oie.getOffs(), true);
			DataDeSerializer dds = new DataDeSerializer(_raf, cache, _node);
			return dds.readObject();
		} catch (Exception e) {
			throw new JDOObjectNotFoundException("ERROR reading object: " + Util.oidToString(oid));
		}
	}


	@Override
	public void close() {
		DatabaseLogger.debugPrintln(1, "Closing DB file: " + _node.getDbPath());
//		_raf.seekPage(_lastAllocPage1);
//		_raf.writeInt(_raf.getLastAllocatedPage());
		_objectWriter.close();
//		_raf.close();
	}

	@Override
	public void postCommit() {
		int oidPage = _oidIndex.write();
		int schemaPage1 = _schemaIndex.write();
		writeMainPage(_userPage1, oidPage, schemaPage1, _indexPage1);
		_objectWriter.flush();
		_raf.flush(); //TODO still necessary??? _objectWriter is already flushed...
	}

	/**
	 * Defines an index and populates it. All objects are put into the cache. This is not 
	 * necessarily useful, but it is a one-off operation. Otherwise we would need a special
	 * purpose implementation of the deserializer, which would have the need for a cache removed.
	 */
	@Override
	public void defineIndex(ZooClassDef cls, ZooFieldDef field, boolean isUnique, 
			AbstractCache cache) {
		SchemaIndexEntry se = _schemaIndex.getSchema(cls.getOid());
		se.defineIndex(cls, field, isUnique);
		
		//fill index with existing objects
		PagedPosIndex ind = se.getObjectIndex();
		Iterator<FilePos> iter = ind.posIterator();
		try {
			while (iter.hasNext()) {
                FilePos oie = iter.next();
		        DataDeSerializer dds = new DataDeSerializer(_raf, cache, _node);
				_raf.seekPage(oie.getPage(), oie.getOffs(), true);
				PersistenceCapableImpl pci = dds.readObject();
			}
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("Error reading objects.", e);
		}
	}

	@Override
	public boolean removeIndex(ZooClassDef cls, ZooFieldDef field) {
		SchemaIndexEntry e = _schemaIndex.getSchema(cls.getOid());
		return e.removeIndex(field);
	}
}
