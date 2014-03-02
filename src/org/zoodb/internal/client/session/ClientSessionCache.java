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
package org.zoodb.internal.client.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import javax.jdo.ObjectState;

import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Node;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.PrimLongMap;
import org.zoodb.internal.util.PrimLongMapLI;
import org.zoodb.internal.util.PrimLongMapLISoft;
import org.zoodb.internal.util.PrimLongMapLIWeak;
import org.zoodb.internal.util.PrimLongMap.PLMValueIterator;

public class ClientSessionCache implements AbstractCache {
	
	//Do not use list to indicate properties! Instead of 1 bit it, lists require 20-30 bytes per entry!
	//TODO Optimize PrimLongTreeMap further? -> HashMaps don't scale!!! (because of the internal array)
//    private final PrimLongMapLI<ZooPCImpl> objs = 
//    	new PrimLongMapLI<ZooPCImpl>();
//    private final PrimLongMapLISoft<ZooPCImpl> objs = 
    private final PrimLongMap<ZooPCImpl> objs; 
	
	private final PrimLongMapLI<ZooClassDef> schemata = 
		new PrimLongMapLI<ZooClassDef>();
	//TODO move into node-cache
	private final HashMap<Node, HashMap<Class<?>, ZooClassDef>> nodeSchemata = 
		new HashMap<Node, HashMap<Class<?>, ZooClassDef>>();
	
	/**
	 * Set of dirty objects. This has two advantages.
	 * 1) It is much faster if only few objects need to be committed while there are many
	 *    clean objects.
	 * 2) It allows some kind of clustering (EXPERIMENTAL!!!) by sorting objects by OID.
	 * 
	 * dirtyObject may include deleted objects!
	 */
	private final ArrayList<ZooPCImpl> dirtyObjects = new ArrayList<ZooPCImpl>();
	private final PrimLongMapLI<ZooPCImpl> deletedObjects = new PrimLongMapLI<ZooPCImpl>();

	private final ArrayList<GenericObject> dirtyGenObjects = new ArrayList<GenericObject>();
	private final PrimLongMapLI<GenericObject> genericObjects = new PrimLongMapLI<GenericObject>();
	
	private final Session session;

	private ZooClassDef metaSchema;
	
	public ClientSessionCache(Session session) {
		this.session = session;
		
		switch (session.getConfig().getCacheMode()) {
		case WEAK: objs = new PrimLongMapLIWeak<ZooPCImpl>(); break; 
		case SOFT: objs = new PrimLongMapLISoft<ZooPCImpl>(); break;
		case PIN: objs = new PrimLongMapLI<ZooPCImpl>(); break;
		default:
			throw new UnsupportedOperationException();
		}

		
		ZooClassDef zpc = ZooClassDef.bootstrapZooPCImpl();
		metaSchema = ZooClassDef.bootstrapZooClassDef();
		metaSchema.initProvidedContext(session, session.getPrimaryNode());
		schemata.put(zpc.getOid(), zpc);
		schemata.put(metaSchema.getOid(), metaSchema);
	}
	
	public Session getSession() {
		return session;
	}


	@Override
	public void rollback() {
		//TODO refresh cleans?  may have changed in DB?
		//Maybe set them all to hollow instead? //TODO

	    //refresh schemata
        //Reloading needs to be in a separate loop. We first need to remove all from the cache
        //before reloading them. Reloading may implicitly load dirty super-classes, which would
        //fail if they are still in the cache and marked as dirty.
        ArrayList<ZooClassDef> schemaToRefresh = new ArrayList<ZooClassDef>();
        ArrayList<ZooClassDef> schemaToRemove = new ArrayList<ZooClassDef>();
        for (ZooClassDef cs: schemata.values()) {
        	if (cs.jdoZooIsDirty()) {
        		if (cs.jdoZooIsNew()) {
        		    schemaToRemove.add(cs);
        		} else {
        			schemaToRefresh.add(cs);
        		}
        	}
        	
        }
        for (ZooClassDef cs: schemaToRemove) {
            schemata.remove(cs.jdoZooGetOid());
            nodeSchemata.get(cs.jdoZooGetNode()).remove(cs.getJavaClass());
        }
        for (ZooClassDef cs: schemaToRefresh) {
            session.getSchemaManager().refreshSchema(cs);
        }
        
	    //TODO Maybe we should simply refresh the whole cache instead of setting them to hollow.
        //This doesn't matter for embedded databases, but for client/server, we could benefit from
        //group-refreshing(loading) all dirty objects 
        for (ZooPCImpl co: dirtyObjects) {
	    	if (co.jdoZooIsDirty()) { // i.e. not refreshed
	    		if (co.jdoZooIsNew()) {
	    			//remove co
	    			objs.remove(co.jdoZooGetOid());
	    		} else {
	    			co.jdoZooMarkHollow();
	    		}
	    	}
        }
        for (ZooPCImpl co: deletedObjects.values()) {
	    	if (co.jdoZooIsDirty()) { // i.e. not refreshed
	    		if (co.jdoZooIsNew()) {
	    			//remove co
	    			objs.remove(co.jdoZooGetOid());
	    		} else {
	    			co.jdoZooMarkHollow();
	    		}
	    	}
        }
	    
		dirtyObjects.clear();
		deletedObjects.clear();
		
        //generic objects
        for (GenericObject go: dirtyGenObjects) {
        	if (go.isNew()) {
        		go.setDeleted(true); //prevent further access to it through existing references
        		genericObjects.remove(go.getOid());
        		continue;
        	}
        	go.setHollow();
        }
        for (GenericObject go: genericObjects.values()) {
        	go.setHollow();
        }
        dirtyGenObjects.clear();
	}


	@Override
	public final void markPersistent(ZooPCImpl pc, long oid, Node node, ZooClassDef clsDef) {
		if (pc.jdoZooIsDeleted()) {
			throw new UnsupportedOperationException("Make it persistent again");
			//TODO implement
		}
		if (pc.jdoZooIsPersistent()) {
			//ignore
			return;
		}
		
		addToCache(pc, clsDef, oid, ObjectState.PERSISTENT_NEW);
	}


	public final void makeTransient(ZooPCImpl pc) {
		//remove it
		if (objs.remove(pc.jdoZooGetOid()) == null) {
			throw DBLogger.newFatal("Object is not in cache.");
		}
		//update
		pc.jdoZooMarkTransient();
	}


	@Override
	public final void addToCache(ZooPCImpl obj, ZooClassDef classDef, long oid, 
			ObjectState state) {
    	obj.jdoZooInit(state, classDef.getProvidedContext(), oid);
		//TODO call newInstance elsewhere
		//obj.jdoReplaceStateManager(co);
		objs.put(obj.jdoZooGetOid(), obj);
	}
	
	
	@Override
	public final ZooPCImpl findCoByOID(long oid) {
		return objs.get(oid);
	}

	/**
	 * TODO Fix this. Schemata should be kept in a separate cache
	 * for each node!
	 * @param cls
	 * @param node
	 * @return Schema object for a given Java class.
	 */
	@Override
	public ZooClassDef getSchema(Class<?> cls, Node node) {
		ZooClassDef ret = nodeSchemata.get(node).get(cls);
		if (ret == null) {
			if (cls == null) {
				return null;
			}
			//Try virtual/generic schemata
			ret = getSchema(cls.getName());
			if (ret != null) {
				//check (associate also checks compatibility)
				ret.associateJavaTypes(true);
				nodeSchemata.get(node).put(cls, ret);
			}
		}
		return ret;
	}

	@Override
	public ZooClassDef getSchema(String clsName) {
		for (ZooClassDef def: schemata.values()) {
			if (def.getNextVersion() == null && def.getClassName().equals(clsName)) {
				return def;
			}
		}
		return null;
	}

	@Override
	public ZooClassDef getSchema(long schemaOid) {
		return schemata.get(schemaOid);
	}

	/**
	 * Clean out the cache after commit.
	 * TODO keep hollow objects? E.g. references to correct, e.t.c!
	 */
	public void postCommit(boolean retainValues) {
		//TODO later: empty cache (?)
		
		for (ZooPCImpl co: deletedObjects.values()) {
			if (co.jdoZooIsDeleted()) {
				objs.remove(co.jdoZooGetOid());
				co.jdoZooGetContext().notifyEvent(co, ZooInstanceEvent.POST_DELETE);
			}
		}
		
		if (retainValues) {
			for (ZooPCImpl co: dirtyObjects) {
				if (!co.jdoZooIsDeleted()) {
					co.jdoZooMarkClean();
				}
			}
		} else {
			if (objs.size() > 100000) {
				DBLogger.debugPrintln(0, "Cache is getting large. Consider retainValues=true"
						+ " to speed up and avoid expensive eviction.");
			}
            for (ZooPCImpl co: objs.values()) {
                if (retainValues || co instanceof ZooClassDef) {
                    co.jdoZooMarkClean();
                } else {
                    co.jdoZooEvict();
                }
                co.jdoZooGetContext().notifyEvent(co, ZooInstanceEvent.POST_STORE);
            }
		}
		dirtyObjects.clear();
		deletedObjects.clear();
		
		//generic objects
        for (GenericObject go: dirtyGenObjects) {
        	if (go.isDeleted()) {
        		genericObjects.remove(go.getOid());
        		continue;
        	}
        	go.setClean();
        }
        for (GenericObject go: genericObjects.values()) {
        	if (!retainValues) {
        		go.setHollow();
        	}
        }
        dirtyGenObjects.clear();

        //schema
		Iterator<ZooClassDef> iterS = schemata.values().iterator();
		for (; iterS.hasNext(); ) {
			ZooClassDef cs = iterS.next();
			if (cs.jdoZooIsDeleted()) {
				iterS.remove();
        		nodeSchemata.get(cs.jdoZooGetNode()).remove(cs.getJavaClass());
				continue;
			}
			//keep in cache???
			cs.jdoZooMarkClean();  //TODO remove if cache is flushed -> retainValues!!!!!
		}
	}

	/**
	 * @return List of all cached schema objects (clean, new, deleted, dirty).
	 */
	public Collection<ZooClassDef> getSchemata() {
		return schemata.values();
	}
	
	public void addSchema(ZooClassDef clsDef, boolean isLoaded, Node node) {
		ObjectState state;
		if (isLoaded) {
			state = ObjectState.PERSISTENT_CLEAN;
		} else {
			state = ObjectState.PERSISTENT_NEW;
		}
		//TODO avoid setting the OID here a second time, seems silly...
    	clsDef.jdoZooInit(state, metaSchema.getProvidedContext(), clsDef.getOid());
		clsDef.initProvidedContext(session, node);
		schemata.put(clsDef.getOid(), clsDef);
		if (clsDef.getNextVersion() == null && clsDef.getJavaClass() != null) {
			nodeSchemata.get(node).put(clsDef.getJavaClass(), clsDef);
		}
		objs.put(clsDef.getOid(), clsDef);
	}
	
	public void updateSchema(ZooClassDef clsDef, Class<?> oldCls, Class<?> newCls) {
		Node node = clsDef.jdoZooGetNode();
		//Removal may return null if class was previously stored a 'null', which is non-unique.
		nodeSchemata.get(node).remove(oldCls);
		if (newCls != null) {
			nodeSchemata.get(node).put(newCls, clsDef);
		}
	}

	public Collection<ZooPCImpl> getAllObjects() {
		return objs.values();
	}

    public void close() {
        objs.clear();
        schemata.clear();
        nodeSchemata.clear();
    }


    public void evictAll() {
        for (ZooPCImpl co: objs.values()) {
            if (!co.jdoZooIsDirty()) {
                co.jdoZooEvict();
            }
        }
    }

    public void evictAll(boolean subClasses, Class<?> cls) {
        for (ZooPCImpl co: objs.values()) {
            if (!co.jdoZooIsDirty() && (co.jdoZooGetClassDef().getJavaClass() == cls || 
                    (subClasses && cls.isAssignableFrom(co.jdoZooGetClassDef().getJavaClass())))) {
                co.jdoZooEvict();
            }
        }
    }

	public void addNode(Node node) {
		nodeSchemata.put(node, new HashMap<Class<?>, ZooClassDef>());
		nodeSchemata.get(node).put(ZooClassDef.class, metaSchema);
	}

	public CloseableIterator<ZooPCImpl> iterator(ZooClassDef def, boolean subClasses, 
			ObjectState state) {
		return new CacheIterator(objs.values().iterator(), def, subClasses, state);
	}
	
	
	private static class CacheIterator implements CloseableIterator<ZooPCImpl> {

		private ZooPCImpl next = null;
		private final PLMValueIterator<ZooPCImpl> iter;
		private final ZooClassDef cls;
		private final boolean subClasses;
		private final ObjectState state;
		
		private CacheIterator(Iterator<ZooPCImpl> iter, 
				ZooClassDef cls, boolean subClasses, ObjectState state) {
			this.iter = (PLMValueIterator<ZooPCImpl>) iter;
			this.cls = cls;
			this.subClasses = subClasses;
			this.state = state;
			//find first object
			next();
		}

		@Override
		public void refresh() {
		    // nothing to do
		}
		
		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public ZooPCImpl next() {
			ZooPCImpl ret = next;
			ZooPCImpl co = null;
			final boolean subClasses = this.subClasses;
			while (iter.hasNextEntry()) {
				co = iter.nextValue();
				ZooClassDef defCand = co.jdoZooGetClassDef();
				if (defCand == cls || (subClasses && cls.hasSuperClass(cls))) {
					if (co.jdoZooHasState(state)) {
						next = co;
						return ret;
					}
				}
			}
			next = null;
			return ret;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() {
			// nothing to do
		}
	}

	/**
	 * This sets the meta schema object for this session. It is the instance of
	 * ZooClassDef that represents its own schema.
	 * @param def
	 */
	public void setRootSchema(ZooClassDef def) {
		//TODO this is a bit funny, but we leave it for now.
		//Ideally, we would not have to reset the metaClass, but the second/loaded version
		//contains additional info such as node/session and is correctly referenced.
		//The original version is only used to initially load any schema from the database.
		metaSchema = def;
	}

	public void notifyDirty(ZooPCImpl pc) {
		dirtyObjects.add(pc);
	}
	
	public ArrayList<ZooPCImpl> getDirtyObjects() {
		return dirtyObjects;
	}

	public void notifyDelete(ZooPCImpl pc) {
		deletedObjects.put(pc.jdoZooGetOid(), pc);
	}
	
	public PrimLongMapLI<ZooPCImpl>.PrimLongValues getDeletedObjects() {
		return deletedObjects.values();
	}

    public void addGeneric(GenericObject genericObject) {
    	if (genericObject.isDirty()) {
    		dirtyGenObjects.add(genericObject);
    	}
    	genericObjects.put(genericObject.getOid(), genericObject);
    }

    public ArrayList<GenericObject> getDirtyGenericObjects() {
        return dirtyGenObjects;
    }

    @Override
    public GenericObject getGeneric(long oid) {
    	return genericObjects.get(oid);
    }
}
