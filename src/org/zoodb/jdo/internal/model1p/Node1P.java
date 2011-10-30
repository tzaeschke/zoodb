package org.zoodb.jdo.internal.model1p;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.OidBuffer;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.client.CachedObject.CachedSchema;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.internal.server.DiskAccess;
import org.zoodb.jdo.internal.server.DiskAccessOneFile;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class Node1P extends Node {

	private ClientSessionCache commonCache;
	private OidBuffer oidBuffer;
	private DiskAccess disk;

	public Node1P(String dbPath, ClientSessionCache cache) {
		super(dbPath);
		oidBuffer = new OidBuffer1P(this);
		commonCache = cache;
	}
	
	@Override
	public void connect() {
		disk = new DiskAccessOneFile(this, commonCache);

		//load all schema data
		Collection<ZooClassDef> defs = disk.readSchemaAll();
		for (ZooClassDef def: defs) {
			def.associateJavaTypes();
			commonCache.addSchema(def, true, this);
		}
	}
	
	public ZooClassDef loadSchema(String clsName, ZooClassDef defSuper) {
		ZooClassDef def = disk.readSchema(clsName, defSuper);
		if (def != null) {
			def.associateJavaTypes();
			commonCache.addSchema(def, true, this);
		}
		return def;
	}
	
	@Override
	public OidBuffer getOidBuffer() {
		return oidBuffer;
	}

	/**
	 * Only used by OidBuffer.
	 * @return DiskAccess instance.
	 */
	DiskAccess getDiskAccess() {
		return disk;
	}

	@Override
	public void commit() {
		//create new schemata
		Collection<CachedObject.CachedSchema> schemata = commonCache.getSchemata(this);
		for (CachedObject.CachedSchema cs: schemata) {
			if (cs.isDeleted()) continue;
			if (cs.isNew() || cs.isDirty()) {
				checkSchemaFields(cs.getSchema(), schemata);
				disk.writeSchema(cs.getSchema(), cs.isNew(), cs.getOID());
			}
		}
		
		//objects
		//TODO
		//We create this Map anew on every call. We don't clear individual list. This is expensive, 
		//and the large arrays may become a memory leak.
		IdentityHashMap<ZooClassDef, ArrayList<PersistenceCapableImpl>> toWrite = 
			new IdentityHashMap<ZooClassDef, ArrayList<PersistenceCapableImpl>>();
		IdentityHashMap<ZooClassDef, ArrayList<PersistenceCapableImpl>> toDelete = 
			new IdentityHashMap<ZooClassDef, ArrayList<PersistenceCapableImpl>>();
		for (PersistenceCapableImpl co: commonCache.getAllObjects()) {
		    if (!co.jdoZooIsDirty() || co.jdoZooGetNode() != this) {
		        continue;
		    }
			if (co.jdoZooIsDeleted()) {
				ArrayList<PersistenceCapableImpl> list = toDelete.get(co.jdoZooGetClassDef());
				if (list == null) {
					//TODO use BucketArrayList
				    //TODO or count instances of each class in cache and use this to initialize Arraylist here???
					list = new ArrayList<PersistenceCapableImpl>();
					toDelete.put(co.jdoZooGetClassDef(), list);
				}
				list.add(co);
			} else {
				ArrayList<PersistenceCapableImpl> list = toWrite.get(co.jdoZooGetClassDef());
				if (list == null) {
					//TODO use BucketArrayList
				    //TODO or count instances of each class in cache and use this to initialize Arraylist here???
					list = new ArrayList<PersistenceCapableImpl>();
					toWrite.put(co.jdoZooGetClassDef(), list);
				}
				list.add(co);
			}
		}

		//Deleting objects class-wise reduces schema index look-ups (negligible?) and allows batching. 
		for (Entry<ZooClassDef, ArrayList<PersistenceCapableImpl>> entry: toDelete.entrySet()) {
			ZooClassDef clsDef = entry.getKey();
			//Ignore instances of deleted classes, there is a dropInstances for them
			if (!clsDef.jdoIsDeleted() && !commonCache.getCachedSchema(clsDef.getJavaClass(), this).isDeleted()) {
				disk.deleteObjects(clsDef.getOid(), entry.getValue());
			}
			entry.setValue(null);
		}

		//Writing the objects class-wise allows easier filling of pages. 
		for (Entry<ZooClassDef, ArrayList<PersistenceCapableImpl>> entry: toWrite.entrySet()) {
			ZooClassDef clsDef = entry.getKey();
			disk.writeObjects(clsDef, entry.getValue());
			entry.setValue(null);
		}

		//delete schemata
		for (CachedObject.CachedSchema cs: schemata) {
			if (cs.isDeleted() && !cs.isNew()) {
				disk.deleteSchema(cs.getSchema());
			}
		}
		
		disk.commit();
		
		commonCache.postCommit();
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
	public CloseableIterator<PersistenceCapableImpl> loadAllInstances(ZooClassDef def, 
            boolean loadFromCache) {
		return disk.readAllObjects(def.getOid(), loadFromCache);
	}

	@Override
	public PersistenceCapableImpl loadInstanceById(long oid) {
		PersistenceCapableImpl pc = disk.readObject(oid);
		//put into local cache (?) -> is currently done in deserializer
		return pc;
	}
	
	@Override
	public void refreshObject(PersistenceCapableImpl pc) {
		disk.readObject(pc);
	}
	
	@Override
	public final void makePersistent(PersistenceCapableImpl obj) {
	    CachedSchema cs = commonCache.getCachedSchema(obj.getClass(), this);
	    ZooClassDef clsDef;
	    if (cs == null || cs.isDeleted()) {
	    	Session s = commonCache.getSession();
	    	if (s.getPersistenceManagerFactory().getAutoCreateSchema()) {
	    		clsDef = s.getSchemaManager().createSchema(this, obj.getClass()).getSchemaDef();
	    	} else {
	    		throw new JDOUserException("No schema found for object: " + 
	                obj.getClass().getName(), obj);
	    	}
	    } else {
		    clsDef = cs.getClassDef();
	    }
		//allocate OID
		long oid = getOidBuffer().allocateOid();
		//add to cache
		commonCache.markPersistent(obj, oid, this, clsDef);
	}

	@Override
	public void closeConnection() {
		disk.close();
	}

	@Override
	public void defineIndex(ZooClassDef def, ZooFieldDef field, boolean isUnique) {
		disk.defineIndex(def, field, isUnique);
	}

	@Override
	public boolean removeIndex(ZooClassDef def, ZooFieldDef field) {
		return disk.removeIndex(def, field);
	}

	@Override
	public byte readAttrByte(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		//TODO put into local cache (?)
		return disk.readAttrByte(oid, schemaDef, attrHandle);
	}

	@Override
	public short readAttrShort(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrShort(oid, schemaDef, attrHandle);
	}

	@Override
	public int readAttrInt(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrInt(oid, schemaDef, attrHandle);
	}

	@Override
	public long readAttrLong(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrLong(oid, schemaDef, attrHandle);
	}

	@Override
	public boolean readAttrBool(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrBool(oid, schemaDef, attrHandle);
	}

	@Override
	public char readAttrChar(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrChar(oid, schemaDef, attrHandle);
	}

	@Override
	public float readAttrFloat(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrFloat(oid, schemaDef, attrHandle);
	}

	@Override
	public double readAttrDouble(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrDouble(oid, schemaDef, attrHandle);
	}

	@Override
	public String readAttrString(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrString(oid, schemaDef, attrHandle);
	}

	@Override
	public Date readAttrDate(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrDate(oid, schemaDef, attrHandle);
	}

	@Override
	public long readAttrRefOid(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle) {
		return disk.readAttrRefOid(oid, schemaDef, attrHandle);
	}
	
	@Override
	public Iterator<PersistenceCapableImpl> readObjectFromIndex( ZooFieldDef field, 
			long minValue, long maxValue, boolean loadFromCache) {
		return disk.readObjectFromIndex(field, minValue, maxValue, loadFromCache);
	}

	@Override
	public int getStatsPageWriteCount() {
		return disk.statsPageWriteCount();
	}

    @Override
    public String checkDb() {
        return disk.checkDb();
    }

	@Override
	public void dropInstances(ZooClassDef def) {
		disk.dropInstances(def);
	}

	@Override
	public void defineSchema(ZooClassDef def) {
		disk.defineSchema(def);
	}

	@Override
	public void undefineSchema(ZooClassDef def) {
		disk.undefineSchema(def);
	}
}
