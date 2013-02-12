/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal.model1p;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import javax.jdo.JDOUserException;
import javax.jdo.listener.DeleteCallback;
import javax.jdo.listener.StoreCallback;

import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.impl.DBStatistics.STATS;
import org.zoodb.jdo.internal.DataDeleteSink;
import org.zoodb.jdo.internal.DataSink;
import org.zoodb.jdo.internal.GenericObject;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.OidBuffer;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooClassProxy;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.ZooHandle;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.internal.server.DiskAccess;
import org.zoodb.jdo.internal.server.DiskAccessOneFile;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.jdo.internal.util.CloseableIterator;

/**
 * 1P (1-process) implementation of the Node interface. 1P means that client and server run
 * in the same process, therefore no inter-process communication is required and the Node1P 
 * implementation can implement direct communication between client (JDO) and database.   
 * 
 * @author Tilmann Zaschke
 */
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
			if (def.getJavaClass()==ZooClassDef.class) {
				def.initProvidedContext(null, commonCache.getSession(), this);
				commonCache.setRootSchema(def);
			}
		}
		//more bootstrapping for brand new databases: enforce writing of root schemata
		if (defs.size()==2) {
			for (ZooClassDef def: defs) {
				commonCache.addSchema(def, false, this);
			}			
		} else {
			for (ZooClassDef def: defs) {
				commonCache.addSchema(def, true, this);
			}
		}
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
		try {
			write();
		} catch (JDOUserException e) {
			//reset sinks
			Collection<ZooClassDef> schemata = commonCache.getSchemata(this);
	        for (ZooClassDef cs: schemata) {
	            cs.getProvidedContext().getDataSink().reset();
	            cs.getProvidedContext().getDataDeleteSink().reset();
	        }		
			throw e;
		}
		
		disk.commit();
		
		commonCache.postCommit();
	}

	private void write() {
		//create new schemata
		Collection<ZooClassDef> schemata = commonCache.getSchemata(this);
		for (ZooClassDef cs: schemata) {
			if (cs.jdoZooIsDeleted()) continue;
			if (cs.jdoZooIsNew() || cs.jdoZooIsDirty()) {
				checkSchemaFields(cs, schemata);
			}
		}
		
		//First delete
		for (ZooPCImpl co: commonCache.getDeletedObjects()) {
		    if (!co.jdoZooIsDirty() || co.jdoZooGetNode() != this) {
		    	throw new IllegalStateException("State=");
		    }
			if (co.jdoZooIsDeleted()) {
				if (co.jdoZooIsNew()) {
					//ignore
					continue;
				}
	            if (co.jdoZooGetClassDef().jdoZooIsDeleted()) {
	                //Ignore instances of deleted classes, there is a dropInstances for them
	                continue;
	            }
				if (co instanceof DeleteCallback) {
					((DeleteCallback)co).jdoPreDelete();
				}
				co.jdoZooGetContext().notifyEvent(co, ZooInstanceEvent.PRE_DELETE);
	            co.jdoZooGetContext().getDataDeleteSink().delete(co);
			} else {
		    	throw new IllegalStateException("State=");
			}
		}
		//flush sinks
        for (ZooClassDef cs: schemata) {
            cs.getProvidedContext().getDataDeleteSink().flush();
        }		

        //Then update. This matters for unique indices where deletion must occur before updates.
		for (ZooPCImpl co: commonCache.getDirtyObjects()) {
		    if (!co.jdoZooIsDirty() || co.jdoZooGetNode() != this) {
		    	//can happen when object are refreshed after being marked dirty? //TODO
		    	//throw new IllegalStateException("State=");
		        continue;
		    }
			if (!co.jdoZooIsDeleted()) {
				if (co instanceof StoreCallback) {
					((StoreCallback)co).jdoPreStore();
				}
				co.jdoZooGetContext().notifyEvent(co, ZooInstanceEvent.PRE_STORE);
			    co.jdoZooGetContext().getDataSink().write(co);
			}
		}

		//generic objects
		if (!commonCache.getDirtyGenericObjects().isEmpty()) {
    		for (GenericObject go: commonCache.getDirtyGenericObjects()) {
    		    go.toStream();
                go.getClassDef().getProvidedContext().getDataSink().writeGeneric(go);
    		}
		}
		
		//flush sinks
        for (ZooClassDef cs: schemata) {
            cs.getProvidedContext().getDataSink().flush();
        }		

		//delete schemata
		for (ZooClassDef cs: schemata) {
			if (cs.jdoZooIsDeleted() && !cs.jdoZooIsNew()) {
				disk.deleteSchema(cs);
			}
		}
	}
	
	
	@Override
	public void revert() {
		disk.revert();
	}
	
	/**
	 * Check the fields defined in this class.
	 * @param schema
	 * @param schemata 
	 */
	private void checkSchemaFields(ZooClassDef schema, Collection<ZooClassDef> cachedSchemata) {
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
    public CloseableIterator<ZooPCImpl> loadAllInstances(ZooClassProxy def, 
            boolean loadFromCache) {
        return disk.readAllObjects(def.getSchemaId(), loadFromCache);
    }

    @Override
    public CloseableIterator<ZooHandle> oidIterator(ZooClassProxy px, boolean subClasses) {
        return disk.oidIterator(px, subClasses);
    }

	@Override
	public ZooPCImpl loadInstanceById(long oid) {
		ZooPCImpl pc = disk.readObject(oid);
		//put into local cache (?) -> is currently done in deserializer
		return pc;
	}
	
	@Override
	public void refreshObject(ZooPCImpl pc) {
		disk.readObject(pc);
	}
	
	@Override
	public void refreshSchema(ZooClassDef def) {
		disk.refreshSchema(def);
		if (def.getJavaClass() == null) {
			def.associateJavaTypes();
		}
		if (commonCache.getSchema(def.getOid()) == null) {
			//can happen if user calls schema.refresh and schema is not loaded.
			//can that really happen????
			commonCache.addSchema(def, true, this);
		}
		def.jdoZooMarkClean();
		ZooClassDef sup = def.getSuperDef();
		while (sup != null && sup.getJavaClass() == null) {
			sup.associateJavaTypes();
			commonCache.addSchema(sup, true, this);
			sup = sup.getSuperDef();
		}
	}
	
	@Override
	public final void makePersistent(ZooPCImpl obj) {
	    ZooClassDef cs = commonCache.getSchema(obj.getClass(), this);
	    if (cs == null || cs.jdoZooIsDeleted()) {
	    	Session s = commonCache.getSession();
	    	if (s.getPersistenceManagerFactory().getAutoCreateSchema()) {
	    		cs = s.getSchemaManager().createSchema(this, obj.getClass()).getSchemaDef();
	    	} else {
	    		throw new JDOUserException("No schema found for object: " + 
	                obj.getClass().getName(), obj);
	    	}
	    }
		//allocate OID
		long oid = getOidBuffer().allocateOid();
		//add to cache
		commonCache.markPersistent(obj, oid, this, cs);
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
	public Iterator<ZooPCImpl> readObjectFromIndex( ZooFieldDef field, 
			long minValue, long maxValue, boolean loadFromCache) {
		return disk.readObjectFromIndex(field, minValue, maxValue, loadFromCache);
	}

	@Override
	public int getStats(STATS stats) {
		return disk.getStats(stats);
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
	public void newSchemaVersion(ZooClassDef defOld, ZooClassDef defNew) {
		disk.newSchemaVersion(defOld, defNew);
	}

	@Override
	public void undefineSchema(ZooClassDef def) {
		disk.undefineSchema(def);
	}

	@Override
	public void renameSchema(ZooClassDef def, String newName) {
		disk.renameSchema(def, newName);
	}

	@Override
	public long getSchemaForObject(long oid) {
		return disk.getObjectClass(oid);
	}

    public SchemaIndexEntry getSchemaIE(ZooClassDef def) {
        return disk.getSchemaIE(def);
    }
    
    @Override 
    public DataSink createDataSink(ZooClassDef clsDef) {
        return new DataSink1P(this, commonCache, clsDef, disk.getWriter(clsDef));
    }
    
    @Override 
    public DataDeleteSink createDataDeleteSink(ZooClassDef clsDef) {
        PagedOidIndex oidIndex = disk.getOidIndex();
        return new DataDeleteSink1P(this, commonCache, clsDef, oidIndex);
    }

	@Override
	public Session getSession() {
		return commonCache.getSession();
	}
}
