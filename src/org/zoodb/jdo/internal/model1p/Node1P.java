package org.zoodb.jdo.internal.model1p;

import java.util.Collection;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.OidBuffer;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.client.CachedObject.CachedSchema;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.internal.server.DiskAccess;
import org.zoodb.jdo.internal.server.DiskAccessOneFile;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class Node1P extends Node {

	private ClientSessionCache _commonCache;
	private OidBuffer _oidBuffer;
	private DiskAccess _disk;

	public Node1P(String dbPath, ClientSessionCache cache) {
		super(dbPath);
		_oidBuffer = new OidBuffer1P(this);
		_commonCache = cache;
	}
	
	@Override
	public void connect() {
		_disk = new DiskAccessOneFile(this);

		//load all schema data
		Collection<ZooClassDef> defs = _disk.readSchemaAll();
		for (ZooClassDef def: defs) {
			def.associateJavaTypes();
			_commonCache.addSchema(def, true, this);
		}
	}
	
	public ZooClassDef loadSchema(String clsName, ZooClassDef defSuper) {
		ZooClassDef def = _disk.readSchema(clsName, defSuper);
		if (def != null) {
			def.associateJavaTypes();
			_commonCache.addSchema(def, true, this);
		}
		return def;
	}
	
	@Override
	public OidBuffer getOidBuffer() {
		return _oidBuffer;
	}

	/**
	 * Only used by OidBuffer.
	 * @return DiskAccess instance.
	 */
	DiskAccess getDiskAccess() {
		return _disk;
	}

	@Override
	public void commit() {
		//create new schemata
		Collection<CachedObject.CachedSchema> schemata = _commonCache.getSchemata(this);
		for (CachedObject.CachedSchema cs: schemata) {
			if (cs.isDeleted()) continue;
			if (cs.isNew() || cs.isDirty()) {
				checkSchemaFields(cs.getSchema(), schemata);
				_disk.writeSchema(cs.getSchema(), cs.isNew(), cs.getOID());
			}
		}
		
		//objects
		//TODO
		//We create this Map anew on every call. We don't clear individual list. This is expensive, 
		//and the large arrays may become a memory leak.
		Map<Class<?>, List<CachedObject>> toWrite = 
			new IdentityHashMap<Class<?>, List<CachedObject>>();
		Map<Class<?>, List<CachedObject>> toDelete = 
			new IdentityHashMap<Class<?>, List<CachedObject>>();
		for (CachedObject co: _commonCache.getAllObjects()) {
		    if (!co.isDirty() || co.getNode() != this) {
		        continue;
		    }
			if (co.isDeleted()) {
				List<CachedObject> list = toDelete.get(co.obj.getClass());
				if (list == null) {
					//TODO use BucketArrayList
				    //TODO or count instances of each class in cache and use this to initialize Arraylist here???
					list = new LinkedList<CachedObject>();
					toDelete.put(co.obj.getClass(), list);
				}
				list.add(co);
			} else {
				List<CachedObject> list = toWrite.get(co.obj.getClass());
				if (list == null) {
					//TODO use BucketArrayList
				    //TODO or count instances of each class in cache and use this to initialize Arraylist here???
					list = new LinkedList<CachedObject>();
					toWrite.put(co.obj.getClass(), list);
				}
				list.add(co);
			}
		}

		//Deleting objects class-wise reduces schema index look-ups (negligible?) and allows batching. 
		for (Entry<Class<?>, List<CachedObject>> entry: toDelete.entrySet()) {
			ZooClassDef clsDef = _commonCache.getCachedSchema(entry.getKey(), this).getSchema();
			_disk.deleteObjects(clsDef.getOid(), entry.getValue());
		}

		//Writing the objects class-wise allows easier filling of pages. 
		for (Entry<Class<?>, List<CachedObject>> entry: toWrite.entrySet()) {
			ZooClassDef clsDef = _commonCache.getCachedSchema(entry.getKey(), this).getSchema();
			_disk.writeObjects(clsDef, entry.getValue(), _commonCache);
		}

		//delete schemata
		for (CachedObject.CachedSchema cs: schemata) {
			if (cs.isDeleted() && !cs.isNew()) {
				_disk.deleteSchema(cs.getSchema());
			}
		}
		
		_disk.postCommit();
		
		_commonCache.postCommit();
	}

	/**
	 * Check the fields defined in this class.
	 * @param schema
	 * @param schemata 
	 */
	private void checkSchemaFields(ZooClassDef schema, 
			Collection<CachedObject.CachedSchema> cachedSchemata) {
		//do this only now, because only now we can check which field types
		//are really persistent!
		//TODO check for field types that became persistent only now -> error!!
		//--> requires schema evolution.
		schema.associateFCOs(this, cachedSchemata);

//		TODO:
//			- construct fieldDefs here an give them to classDef.
//			- load required field type defs
//			- check cache (the cachedList only contains dirty/new schemata!)
	}

	@Override
	public Iterator<PersistenceCapableImpl> loadAllInstances(Class<?> cls) {
		return _disk.readAllObjects(cls.getName(), _commonCache);
	}

	@Override
	public PersistenceCapableImpl loadInstanceById(long oid) {
		PersistenceCapableImpl pc = _disk.readObject(_commonCache, oid);
		//TODO put into local cache (?) -> is currently done in deserializer
//		_commonCache.addPC(pc, this);
		return pc;
	}
	
	@Override
	public void makePersistent(PersistenceCapableImpl obj) {
	    CachedSchema cs = _commonCache.getCachedSchema(obj.getClass(), this);
	    if (cs == null || cs.isDeleted()) {
	        throw new JDOUserException("No schema found for object: " + 
	                obj.getClass().getName(), obj);
	    }
		//allocate OID
		long oid = getOidBuffer().allocateOid();
		//add to cache
		_commonCache.markPersistent(obj, oid, this);
		//update pc
		obj.jdoReplaceStateManager(_commonCache.getStateManager());//TODO
		obj.jdoZooSetOid(oid);
	}

	@Override
	public void closeConnection() {
		_disk.close();
	}

	@Override
	public void defineIndex(ZooClassDef def, ZooFieldDef field, boolean isUnique) {
		_disk.defineIndex(def, field, isUnique, _commonCache);
	}

	@Override
	public boolean removeIndex(ZooClassDef def, ZooFieldDef field) {
		return _disk.removeIndex(def, field);
	}

	@Override
	public byte readAttrByte(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		//TODO put into local cache (?)
		return _disk.readAttrByte(oid, schemaDef, attrHandle);
	}

	@Override
	public short readAttrShort(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrShort(oid, schemaDef, attrHandle);
	}

	@Override
	public int readAttrInt(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrInt(oid, schemaDef, attrHandle);
	}

	@Override
	public long readAttrLong(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrLong(oid, schemaDef, attrHandle);
	}

	@Override
	public boolean readAttrBool(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrBool(oid, schemaDef, attrHandle);
	}

	@Override
	public char readAttrChar(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrChar(oid, schemaDef, attrHandle);
	}

	@Override
	public float readAttrFloat(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrFloat(oid, schemaDef, attrHandle);
	}

	@Override
	public double readAttrDouble(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrDouble(oid, schemaDef, attrHandle);
	}

	@Override
	public String readAttrString(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrString(oid, schemaDef, attrHandle);
	}

	@Override
	public Date readAttrDate(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrDate(oid, schemaDef, attrHandle);
	}

	@Override
	public long readAttrRefOid(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return _disk.readAttrRefOid(oid, schemaDef, attrHandle);
	}
	
	@Override
	public Iterator<PersistenceCapableImpl> readObjectFromIndex(ZooClassDef clsDef,
			ZooFieldDef field, long minValue, long maxValue, AbstractCache cache) {
		return _disk.readObjectFromIndex(clsDef, field, minValue, maxValue, cache);
	}
}
