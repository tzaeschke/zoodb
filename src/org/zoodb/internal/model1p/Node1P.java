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
package org.zoodb.internal.model1p;

import java.util.Collection;
import java.util.Iterator;

import org.zoodb.api.impl.ZooPCImpl;
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
import org.zoodb.internal.server.DiskAccessOneFile;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.tools.DBStatistics.STATS;

/**
 * 1P (1-process) implementation of the Node interface. 1P means that client and server run
 * in the same process, therefore no inter-process communication is required and the Node1P 
 * implementation can implement direct communication between client (JDO) and database.   
 * 
 * @author Tilmann Zaeschke
 */
public class Node1P extends Node {

	private final ClientSessionCache commonCache;
	private final OidBuffer oidBuffer;
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
		disk.commit();
	}
	
	
	@Override
	public void revert() {
		disk.revert();
	}
	
    @Override
    public CloseableIterator<ZooPCImpl> loadAllInstances(ZooClassProxy def, 
            boolean loadFromCache) {
        return disk.readAllObjects(def.getSchemaId(), loadFromCache);
    }

    @Override
    public CloseableIterator<ZooHandleImpl> oidIterator(ZooClassProxy px, boolean subClasses) {
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
	}
	
	@Override
	public final void makePersistent(ZooPCImpl obj) {
	    ZooClassDef cs = commonCache.getSchema(obj.getClass(), this);
	    if (cs == null || cs.jdoZooIsDeleted()) {
	    	SchemaManager sm = commonCache.getSession().getSchemaManager();
	    	if (sm.getAutoCreateSchema()) {
	    		cs = sm.createSchema(this, obj.getClass()).getSchemaDef();
	    	} else {
	    		throw DBLogger.newUser("No schema found for object: " + obj.getClass().getName());
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
	public void dropInstances(ZooClassProxy def) {
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
	public void undefineSchema(ZooClassProxy def) {
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

	@Override
	public long countInstances(ZooClassProxy clsDef, boolean subClasses) {
		return disk.countInstances(clsDef, subClasses);
	}

	@Override
	public GenericObject readGenericObject(ZooClassDef def, long oid) {
		return disk.readGenericObject(def, oid);
	}

	@Override
	public void deleteSchema(ZooClassDef cs) {
		disk.deleteSchema(cs);
	}

	@Override
	public boolean checkIfObjectExists(long oid) {
		return disk.checkIfObjectExists(oid);
	}
}
