package org.zoodb.jdo.internal.model1p;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.OidBuffer;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.client.CachedObject;
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
		_disk = new DiskAccessOneFile(this);
		_commonCache = cache;
	}
	
//	public boolean isSchemaDefined(Class type) {
//		ISchema s = _disk.readSchema(type);
//		if (s == null) {
//			return false;
//		}
//		return true;
//	}

	public ZooClassDef loadSchema(String clsName, ZooClassDef defSuper) {
		return _disk.readSchema(clsName, defSuper);
	}
	
//	@Override
//	public ISchema locateSchema(Class cls) {
//		if (!isSchemaDefined(cls)) {
//			return null;
//		}
//		//it should now be in the cache
//		return _commonCache.findSchemaInCache(cls, this);
//	}
//
//	@Override
//	public ISchema locateSchema(String className) {
//		//TODO USe classNames internally, rather than classes
//		//TODO remove:
//		Class cls = null;
//		try {
//			cls = Class.forName(className);
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return locateSchema(cls);
//	}
//
//	@Override
//	public ISchema createSchema(Class cls, long oid, boolean isLoaded) {
//		if (!isLoaded && isSchemaDefined(cls)) {
//			throw new JDOUserException(
//					"Schema is already defined: " + cls.getName());
//		}
//		node.getClientCache().addSchema(this, isLoaded);
//		return new SchemaImpl(cls, oid, isLoaded);
//	}

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
		List<CachedObject.CachedSchema> schemata = _commonCache.getSchemata(this);
		for (CachedObject.CachedSchema cs: schemata) {
			if (cs.isDeleted()) continue;
			if (cs.isNew() || cs.isDirty()) {
				checkSchemaFields(cs.getSchema(), schemata);
				_disk.writeSchema(cs.getSchema(), cs.isNew(), cs.getOID());
			}
		}
		
		//objects
		//We create this Map anew on every call. We don't clear individual list. This is expensive, 
		//and the large arrays may become a memory leak.
		Map<Class<?>, List<CachedObject>> toWrite = 
			new IdentityHashMap<Class<?>, List<CachedObject>>();
		for (CachedObject co: _commonCache.getAllObjects()) {
		    if (!co.isDirty() || co.getNode() != this) {
		        continue;
		    }
			if (co.isDeleted()) {
				_disk.deleteObject(co.getObject(), co.getOID());
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

		//Writing the objects class-wise allows easier filling of pages. 
		for (Entry<Class<?>, List<CachedObject>> entry: toWrite.entrySet()) {
			_disk.writeObjects(entry.getKey(), entry.getValue());
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
			List<CachedObject.CachedSchema> cachedSchemata) {
		//do this only now, because only now we can check which field types
		//are really persistent!
		//TODO check for field types that became persistent only now -> error!!
		//--> requires schema evolution.
		schema.constructFields(this, cachedSchemata);

//		TODO:
//			- construct fieldDefs here an give them to classDef.
//			- load required field type defs
//			- check cache (the cachedList only contains dirty/new schemata!)
	}

	@Override
	public Collection<PersistenceCapableImpl> loadAllInstances(Class cls) {
		List<PersistenceCapableImpl> all = _disk.readAllObjects(cls.getName(), _commonCache);
		return all;
	}

	@Override
	public PersistenceCapableImpl loadInstanceById(long oid) {
		//TODO put into local cache (?)
		return _disk.readObject(_commonCache, oid);
	}
	
	@Override
	public void makePersistent(PersistenceCapableImpl obj) {
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
}
