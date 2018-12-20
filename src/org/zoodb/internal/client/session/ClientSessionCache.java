/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Node;
import org.zoodb.internal.ObjectGraphTraverser;
import org.zoodb.internal.OidBuffer;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.PrimLongMap;
import org.zoodb.internal.util.PrimLongMapZ;
import org.zoodb.internal.util.PrimLongMapZSoft;
import org.zoodb.internal.util.PrimLongMapZWeak;

public class ClientSessionCache implements AbstractCache {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(ClientSessionCache.class);

	//Do not use list to indicate properties! Instead of 1 bit it, lists require 20-30 bytes per entry!
	//TODO Optimize PrimLongTreeMap further? -> HashMaps don't scale!!! (because of the internal array)
//    private final PrimLongMapLI<ZooPC> objs = 
//    	new PrimLongMapLI<ZooPC>();
//    private final PrimLongMapLISoft<ZooPC> objs = 
    private final PrimLongMap<ZooPC> objs; 
	
	private final PrimLongMapZ<ZooClassDef> schemata = 
		new PrimLongMapZ<ZooClassDef>();
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
	private final ArrayList<ZooPC> dirtyObjects = new ArrayList<ZooPC>();
	private final PrimLongMapZ<ZooPC> deletedObjects = new PrimLongMapZ<ZooPC>();

	private final ArrayList<GenericObject> dirtyGenObjects = new ArrayList<GenericObject>();
	private final PrimLongMapZ<GenericObject> genericObjects = new PrimLongMapZ<GenericObject>();
	
	private final Session session;
	private final ObjectGraphTraverser ogt;
	private final OidBuffer oidBuffer;

	private ZooClassDef metaSchema;
	
	public ClientSessionCache(Session session, Node primary) {
		this.session = session;
		this.ogt = new ObjectGraphTraverser(this); 
		this.oidBuffer = primary.getOidBuffer();
		
		switch (session.getConfig().getCacheMode()) {
		case WEAK: objs = new PrimLongMapZWeak<ZooPC>(); break; 
		case SOFT: objs = new PrimLongMapZSoft<ZooPC>(); break;
		case PIN: objs = new PrimLongMapZ<ZooPC>(); break;
		default:
			throw new UnsupportedOperationException();
		}
		
		ZooClassDef zpc = ZooClassDef.bootstrapZooPCImpl();
		metaSchema = ZooClassDef.bootstrapZooClassDef();
		metaSchema.initProvidedContext(session, null);//session.getPrimaryNode());
		schemata.put(zpc.getOid(), zpc);
		schemata.put(metaSchema.getOid(), metaSchema);
	}
	
	public Session getSession() {
		return session;
	}


	@Override
	public void rollback() {
		int logSizeObjBefore = objs.size();
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
        			//TODO remove? This is never used..., See also Test038/issue 54
        			schemaToRefresh.add(cs);
        		}
        	}
        	
        }
        for (ZooClassDef cs: schemaToRemove) {
            schemata.remove(cs.jdoZooGetOid());
            nodeSchemata.get(cs.jdoZooGetNode()).remove(cs.getJavaClass());
        }
        
	    //TODO Maybe we should simply refresh the whole cache instead of setting them to hollow.
        //This doesn't matter for embedded databases, but for client/server, we could benefit from
        //group-refreshing(loading) all dirty objects 
        for (ZooPC co: dirtyObjects) {
	    	if (co.jdoZooIsDirty()) { // i.e. not refreshed
	    		if (co.jdoZooIsNew()) {
	    			//remove co
	    			objs.remove(co.jdoZooGetOid());
	    			co.jdoZooMarkTransient();
	    		} else {
	    			co.jdoZooMarkHollow();
	    		}
	    	}
        }
        for (ZooPC co: deletedObjects.values()) {
	    	if (co.jdoZooIsDirty()) { // i.e. not refreshed
	    		if (co.jdoZooIsNew()) {
	    			//remove co
	    			objs.remove(co.jdoZooGetOid());
	    			co.jdoZooMarkTransient();
	    		} else {
	    			co.jdoZooMarkHollow();
	    		}
	    	}
        }
	    
		dirtyObjects.clear();
		deletedObjects.clear();
		
        //generic objects
        for (GenericObject go: dirtyGenObjects) {
        	if (go.jdoZooIsNew()) {
        		//TODO really? Make it transient and remove from list?
        		//     Or rather keep it in list? --> Always persistent?
        		//go.invalidate(); //prevent further access to it through existing references
        		genericObjects.remove(go.getOid());
    			go.jdoZooMarkTransient();
        		continue;
        	}
        	go.jdoZooMarkHollow();
        }
        for (GenericObject go: genericObjects.values()) {
        	go.jdoZooMarkHollow();
        }
        dirtyGenObjects.clear();
		if (Session.LOGGER.isInfoEnabled()) {
			int logSizeObjAfter = objs.size();
			Session.LOGGER.info("ClientCache.rollback() - Cache size before/after: {} / {}", 
					logSizeObjBefore, logSizeObjAfter);
		}
	}


	@Override
	public final void markPersistent(ZooPC pc, Node node, ZooClassDef clsDef) {
		if (pc.jdoZooIsDeleted()) {
			throw new UnsupportedOperationException("Make it persistent again");
			//TODO implement
		}
		if (pc.jdoZooIsPersistent()) {
			//ignore
			return;
		}
		
		long oid = pc.jdoZooGetOid();
		if (OidBuffer.isValid(oid)) {
			//We have an OID. This can happen during re-attach or when the OID is explicitly set
			if (pc.jdoZooIsDetached()) {
				//Reattaching object? We need to ensure transitivity. The special case here is
				//Objects that change to CLEAN, because the won't be checked by the OGT.
				if (objs.containsKey(oid)) {
					//We have to check and fail _before_ we change the status of the object.
					throw DBLogger.newUser("The session already contains an object with the same "
							+ "OID (OID conflict): " + oid);
				}	
				if (!pc.jdoZooIsDirty()) {
					ogt.traverse(pc);
				}
				addToCache(pc, clsDef, oid, pc.jdoZooIsDirty() ? 
						ObjectState.PERSISTENT_DIRTY : ObjectState.PERSISTENT_CLEAN);
			} else {
				addToCache(pc, clsDef, oid, ObjectState.PERSISTENT_NEW);
			}
		} else {
			oid = oidBuffer.allocateOid();
			addToCache(pc, clsDef, oid, ObjectState.PERSISTENT_NEW);
		}
	}


	public final void makeTransient(ZooPC pc) {
		//remove it
		if (pc.getClass() == GenericObject.class) {
			if (genericObjects.remove(pc.jdoZooGetOid()) == null) {
				throw DBLogger.newFatalInternal("Object is not in cache.");
			}
		} else if (objs.remove(pc.jdoZooGetOid()) == null) {
			throw DBLogger.newFatalInternal("Object is not in cache.");
		}
		//update
		pc.jdoZooMarkTransient();
	}


	@Override
	public final void addToCache(ZooPC obj, ZooClassDef classDef, long oid, 
			ObjectState state) {
		if (obj.getClass() == GenericObject.class) {
			addGeneric((GenericObject) obj);
			return;
		}
    	obj.jdoZooInit(state, classDef.getProvidedContext(), oid);
		//TODO call newInstance elsewhere
		//obj.jdoReplaceStateManager(co);
		Object result = objs.putIfAbsent(obj.jdoZooGetOid(), obj);
		if (result != null) {
			throw DBLogger.newUser("The session already contains an object with the same "
					+ "OID (OID conflict): " + oid);
		}
	}
	
	
	@Override
	public final ZooPC findCoByOID(long oid) {
		return objs.get(oid);
	}

	/**
	 * TODO Fix this. Schemata should be kept in a separate cache
	 * for each node!
	 * @param cls Class name
	 * @param node Node object
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
	 * @param retainValues retainValues flag
	 * @param detachAllOnCommit detachAllOnCommit flag
	 */
	public void postCommit(boolean retainValues, boolean detachAllOnCommit) {
		int logSizeObjBefore = objs.size();
		long t1 = System.nanoTime();
		//TODO later: empty cache (?)
		
		if (!deletedObjects.isEmpty()) {
			for (ZooPC co: deletedObjects.values()) {
				if (co.jdoZooIsDeleted()) {
					objs.remove(co.jdoZooGetOid());
					co.jdoZooGetContext().notifyEvent(co, ZooInstanceEvent.POST_DELETE);
				}
			}
		}
		
		if (detachAllOnCommit) {
			detachAllOnCommit();
		} else if (retainValues) {
			if (!dirtyObjects.isEmpty()) {
				for (ZooPC co: dirtyObjects) {
					if (!co.jdoZooIsDeleted()) {
						co.jdoZooMarkClean();
					}
				}
			}
		} else {
			if (objs.size() > 100000) {
				LOGGER.warn("Cache is getting large. Consider retainValues=true"
						+ " to speed up and avoid expensive eviction.");
			}
            for (ZooPC co: objs.values()) {
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
		if (!dirtyGenObjects.isEmpty()) {
	        for (GenericObject go: dirtyGenObjects) {
	        	if (go.jdoZooIsDeleted()) {
	        		genericObjects.remove(go.getOid());
	        		continue;
	        	}
	        	go.jdoZooMarkClean();
	        }
		}
		if (!genericObjects.isEmpty()) {
	        for (GenericObject go: genericObjects.values()) {
	        	if (!retainValues) {
	        		go.jdoZooMarkHollow();
	        	}
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
		
		if (Session.LOGGER.isInfoEnabled()) {
			int logSizeObjAfter = objs.size();
			long t2 = System.nanoTime();
			Session.LOGGER.info("ClientCache.postCommit() -- Time= {} ns; Cache size before/after: {} / {}", 
					(t2-t1), logSizeObjBefore, logSizeObjAfter);
		}
	}

	public void detachAllOnCommitMaterialize() {
		//We have to do this twice...
		//First round: ensure that all objs are loaded (non-hollow) with refresh()
		//Second round: Detach.
		//--> Otherwise, the refresh may read-add a referenced object to the cache...
		//--> Also: Materialize has to happen while we still have a DB-lock. 
		Iterator<ZooPC> it = objs.values().iterator();
        while (it.hasNext()) {
        	ZooPC co = it.next();
            if (co instanceof ZooClassDef) {
                co.jdoZooMarkClean();
                co.jdoZooGetContext().notifyEvent(co, ZooInstanceEvent.POST_STORE);
            } else {
                co.jdoZooGetContext().notifyEvent(co, ZooInstanceEvent.PRE_DETACH);
                if (co.jdoZooIsStateHollow()) {
                	//TODO remove this, instead fix DETACH to work with hollow objects
                	co.jdoZooGetNode().refreshObject(co);
                }
            }
        }
        //second round is in detachAllOnCommit()
	}

	private void detachAllOnCommit() {
		Iterator<ZooPC> it = objs.values().iterator();
        while (it.hasNext()) {
        	ZooPC co = it.next();
            if (!(co instanceof ZooClassDef)) {
                co.jdoZooMarkDetached();
                it.remove();
                co.jdoZooGetContext().notifyEvent(co, ZooInstanceEvent.POST_DETACH);
            }
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

	public Collection<ZooPC> getAllObjects() {
		return objs.values();
	}

	public Collection<GenericObject> getAllGenericObjects() {
		return genericObjects.values();
	}

    public void close() {
        objs.clear();
        schemata.clear();
        nodeSchemata.clear();
    }


    public void evictAll() {
        for (ZooPC co: objs.values()) {
            if (!co.jdoZooIsDirty()) {
                co.jdoZooEvict();
            }
        }
    }

    public void evictAll(boolean subClasses, Class<?> cls) {
        for (ZooPC co: objs.values()) {
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

	public CloseableIterator<ZooPC> iterator(ZooClassDef clsDef, boolean subClasses, 
			ObjectState state) {
		if (state == ObjectState.PERSISTENT_NEW) {
			ArrayList<ZooPC> ret = new ArrayList<>();
			for (ZooPC pc: dirtyObjects) {
				ZooClassDef defCand = pc.jdoZooGetClassDef();
				if (defCand == clsDef || (subClasses && defCand.hasSuperClass(clsDef))) {
					if (pc.jdoZooHasState(state)) {
						ret.add(pc);
					}
				}
			}
			return new DummyIterator(ret.iterator());
		} 
		//currently not required
		throw new UnsupportedOperationException();
		//return new CacheIterator(objs.values().iterator(), def, subClasses, stte);
	}
	
	private static class DummyIterator implements CloseableIterator<ZooPC> {
		private final Iterator<ZooPC> iter;
		private DummyIterator(Iterator<ZooPC> iterator) {
			iter = iterator;
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public ZooPC next() {
			return iter.next();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() {
			//ignore
		}
		
	}
	
	/**
	 * This sets the meta schema object for this session. It is the instance of
	 * ZooClassDef that represents its own schema.
	 * @param def Class definition
	 */
	public void setRootSchema(ZooClassDef def) {
		//TODO this is a bit funny, but we leave it for now.
		//Ideally, we would not have to reset the metaClass, but the second/loaded version
		//contains additional info such as node/session and is correctly referenced.
		//The original version is only used to initially load any schema from the database.
		metaSchema = def;
	}

	public void notifyDirty(ZooPC pc) {
		if (pc.getClass() == GenericObject.class) {
			dirtyGenObjects.add((GenericObject) pc);
			return;
		}
		dirtyObjects.add(pc);
	}
	
	public ArrayList<ZooPC> getDirtyObjects() {
		return dirtyObjects;
	}

	public void notifyDelete(ZooPC pc) {
		if (pc.getClass() == GenericObject.class) {
			dirtyGenObjects.add((GenericObject) pc);
			return;
		}
		deletedObjects.put(pc.jdoZooGetOid(), pc);
	}
	
	public PrimLongMapZ<ZooPC>.PrimLongValues getDeletedObjects() {
		return deletedObjects.values();
	}

	@Override
    public void addGeneric(GenericObject genericObject) {
    	genericObjects.put(genericObject.getOid(), genericObject);
    }

    public ArrayList<GenericObject> getDirtyGenericObjects() {
        return dirtyGenObjects;
    }

    @Override
    public GenericObject getGeneric(long oid) {
    	return genericObjects.get(oid);
    }

	public boolean hasDirtyPojos() {
		//ignore generic objects for now, they need no traversing
		return !dirtyObjects.isEmpty();
	}
	
	/**
	 * Traverse the object graph and call makePersistent() on all reachable
	 * objects.
	 */
	public void persistReachableObjects() {
		ogt.traverse();
	}

	/**
	 * Tell the OGT that the object graph has changed and that a new traversal is required.
	 */
	public void flagOGTraversalRequired() {
		ogt.flagTraversalRequired();
	}
}
