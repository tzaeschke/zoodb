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
package org.zoodb.internal.model1p;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.DataDeleteSink;
import org.zoodb.internal.DataSink;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Node;
import org.zoodb.internal.OidBuffer;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.ZooHandleImpl;
import org.zoodb.internal.client.SchemaManager;
import org.zoodb.internal.client.session.ClientSessionCache;
import org.zoodb.internal.server.DiskAccess;
import org.zoodb.internal.server.OptimisticTransactionResult;
import org.zoodb.internal.server.ServerResponse;
import org.zoodb.internal.server.SessionFactory;
import org.zoodb.internal.server.TxObjInfo;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Util;
import org.zoodb.tools.DBStatistics.STATS;

/**
 * 1P (1-process) implementation of the Node interface. 1P means that client and server run
 * in the same process, therefore no inter-process communication is required and the Node1P 
 * implementation can implement direct communication between client (JDO) and database.   
 * 
 * @author Tilmann Zaeschke
 */
public class Node1P extends Node {

	private final OidBuffer oidBuffer;
	private DiskAccess disk;
	private final Session session;

	public Node1P(String dbPath, Session session) {
		super(dbPath);
		this.oidBuffer = new OidBuffer1P(this);
		this.session = session;
	}
	
	@Override
	public void connect() {
		ClientSessionCache commonCache = session.internalGetCache();
		disk = SessionFactory.getSession(this, commonCache);

		//load all schema data
		Collection<ZooClassDef> defs = disk.readSchemaAll();
		for (ZooClassDef def: defs) {
			def.associateJavaTypes();
			if (def.getJavaClass() == ZooClassDef.class) {
				def.initProvidedContext(commonCache.getSession(), this);
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
		
		//To drop all locks
		disk.rollbackTransaction();
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
	public long beginTransaction() {
		return disk.beginTransaction();
	}
	
	@Override
	public OptimisticTransactionResult rollbackTransaction() {
		return disk.rollbackTransaction();
	}
	
	@Override
	public void commit() {
		disk.commit();
	}

	@Override
	public OptimisticTransactionResult checkTxConsistency(ArrayList<TxObjInfo> updates) {
		return disk.checkTxConsistency(updates);
	}
	
	@Override
	public void revert() {
		disk.revert();
	}
	
    @Override
    public CloseableIterator<ZooPC> loadAllInstances(ZooClassProxy def, 
            boolean loadFromCache) {
        return disk.readAllObjects(def.getSchemaId(), loadFromCache);
    }

    @Override
    public CloseableIterator<ZooHandleImpl> oidIterator(ZooClassProxy px, boolean subClasses) {
        return disk.oidIterator(px, subClasses);
    }

	@Override
	public ZooPC loadInstanceById(long oid) {
		ZooPC pc = disk.readObject(oid);
		//put into local cache (?) -> is currently done in deserializer
		return pc;
	}
	
	@Override
	public void refreshObject(ZooPC pc) {
		if (pc.jdoZooIsNew() || (!pc.jdoZooIsStateHollow() && !pc.jdoZooIsTransactional())) {
			//ignore non-persistent objects
			return;
		}
		if (pc.jdoZooIsDeleted()) {
			//deleted objects remain unchanged (!) --> see spec.
			return;
		}
		ServerResponse r = disk.readObject(pc);
		if (r.result() == ServerResponse.RESULT.OBJECT_NOT_FOUND) {
			//must have been deleted
			//We mark it as deleted/transient to allow follow-up commits() to go through.
			//'Transient' is the JDO-spec state of a deleted object after commit()
			//if (!pc.jdoZooIsDeleted() || pc.jdoZooIsPersistent()) {
			pc.jdoZooMarkClean();
			pc.jdoZooEvict();
			session.makeTransient(pc);
			//}
			throw DBLogger.newObjectNotFoundException(
					"Object not found: " + Util.getOidAsString(pc), null, pc);
		}
	}
	
	@Override
	public void refreshSchema(ZooClassDef def) {
		disk.refreshSchema(def);
	}
	
	@Override
	public final void makePersistent(ZooPC obj) {
		ClientSessionCache commonCache = session.internalGetCache();
	    ZooClassDef cs;
	    if (obj.getClass() != GenericObject.class) {
		    cs = commonCache.getSchema(obj.getClass(), this);
		    if (cs == null || cs.jdoZooIsDeleted()) {
		    	SchemaManager sm = session.getSchemaManager();
		    	if (sm.getAutoCreateSchema()) {
		    		cs = sm.createSchema(this, obj.getClass()).getSchemaDef();
		    	} else {
		    		throw DBLogger.newUser("No schema found for object: " + obj.getClass().getName());
		    	}
		    }
	    } else {
	    	cs = obj.jdoZooGetClassDef();
	    }
		//add to cache
		commonCache.markPersistent(obj, this, cs);
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
	public Iterator<ZooPC> readObjectFromIndex( ZooFieldDef field, 
			long minValue, long maxValue, boolean loadFromCache) {
		return disk.readObjectFromIndex(field, minValue, maxValue, loadFromCache);
	}

	@Override
	public long getStats(STATS stats) {
		return disk.getStats(stats);
	}

    @Override
    public String checkDb() {
        return disk.checkDb();
    }

	@Override
	public void dropInstances(ZooClassProxy def) {
		disk.dropInstances(def);
	}

	@Override
	public void defineSchema(ZooClassDef def) {
		disk.defineSchema(def);
	}

	@Override
	public void newSchemaVersion(ZooClassDef defNew) {
		disk.newSchemaVersion(defNew);
	}

	@Override
	public void renameSchema(ZooClassDef def, String newName) {
		disk.renameSchema(def, newName);
	}

	@Override
	public void undefineSchema(ZooClassProxy def) {
		disk.undefineSchema(def);
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
        return new DataSink1P(this, session.internalGetCache(), clsDef, disk.getWriter(clsDef));
    }
    
    @Override 
    public DataDeleteSink createDataDeleteSink(ZooClassDef clsDef) {
        PagedOidIndex oidIndex = disk.getOidIndex();
        return new DataDeleteSink1P(this, clsDef, oidIndex);
    }

	@Override
	public Session getSession() {
		return session;
	}

	@Override
	public long countInstances(ZooClassProxy clsDef, boolean subClasses) {
		return disk.countInstances(clsDef, subClasses);
	}

	@Override
	public GenericObject readGenericObject(ZooClassDef def, long oid) {
		return disk.readGenericObject(def, oid);
	}

	@Override
	public boolean checkIfObjectExists(long oid) {
		return disk.checkIfObjectExists(oid);
	}

	@Override
	public OptimisticTransactionResult beginCommit(ArrayList<TxObjInfo> updates) {
		return disk.beginCommit(updates);
	}
}
