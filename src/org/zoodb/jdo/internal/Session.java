/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.WeakHashMap;

import javax.jdo.JDOFatalException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.listener.DeleteCallback;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.jdo.listener.StoreCallback;

import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.PersistenceManagerFactoryImpl;
import org.zoodb.jdo.PersistenceManagerImpl;
import org.zoodb.jdo.internal.client.SchemaManager;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.DBLogger;
import org.zoodb.jdo.internal.util.IteratorRegistry;
import org.zoodb.jdo.internal.util.MergingIterator;
import org.zoodb.jdo.internal.util.TransientField;

/**
 * The main session class.
 * 
 * @author ztilmann
 *
 */
public class Session implements IteratorRegistry {

	public static final long OID_NOT_ASSIGNED = -1;

	public static final Class<?> PERSISTENT_SUPER = ZooPCImpl.class;
	
	/** Primary node. Also included in the _nodes list. */
	private Node primary;
	/** All connected nodes. Includes the primary node. */
	private final ArrayList<Node> nodes = new ArrayList<Node>();
	private final PersistenceManagerImpl pm;
	private final ClientSessionCache cache;
	private final SchemaManager schemaManager;
	
	private final WeakHashMap<CloseableIterator<?>, Object> extents = 
	    new WeakHashMap<CloseableIterator<?>, Object>(); 
	
	public Session(PersistenceManagerImpl pm, String dbPath) {
		this.pm = pm;
		this.cache = new ClientSessionCache(this);
		this.schemaManager = new SchemaManager(cache);
		this.primary = ZooFactory.get().createNode(dbPath, cache);
		this.nodes.add(primary);
		this.cache.addNode(primary);
		this.primary.connect();
	}
	
	
	public void commit(boolean retainValues) {
		//pre-commit: traverse object tree for transitive persistence
		ObjectGraphTraverser ogt = new ObjectGraphTraverser(pm, cache);
		ogt.traverse();
		
		schemaManager.commit();
		
		try {
			commitInternal();
			for (Node n: nodes) {
				//TODO two-phase commit() !!!
				n.commit();
			}
			cache.postCommit(retainValues);
		} catch (JDOUserException e) {
			//reset sinks
	        for (ZooClassDef cs: cache.getSchemata()) {
	            cs.getProvidedContext().getDataSink().reset();
	            cs.getProvidedContext().getDataDeleteSink().reset();
	        }		
			//allow for retry after user exceptions
			for (Node n: nodes) {
				n.revert();
			}
			throw e;
		}
        
		for (CloseableIterator<?> ext: extents.keySet()) {
		    //TODO
		    //Refresh extents to allow cross-session-border extents.
		    //As a result, extents may skip objects or return objects twice,
		    //but at least they return valid object.
		    //This problem occurs because extents use pos-indices.
		    //TODO Ideally we should use a OID based class-index. See design.txt.
		    ext.refresh();
		}
		DBLogger.debugPrintln(2, "FIXME: 2-phase Session.commit()");
	}

	
	private void commitInternal() {
		//create new schemata
		Collection<ZooClassDef> schemata = cache.getSchemata();
		for (ZooClassDef cs: schemata) {
			if (cs.jdoZooIsDeleted()) continue;
			if (cs.jdoZooIsNew() || cs.jdoZooIsDirty()) {
				checkSchemaFields(cs, schemata);
			}
		}
		
		//First delete
		for (ZooPCImpl co: cache.getDeletedObjects()) {
		    if (!co.jdoZooIsDirty()) {
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
		//generic objects
		if (!cache.getDirtyGenericObjects().isEmpty()) {
    		for (GenericObject go: cache.getDirtyGenericObjects()) {
    			if (go.isDeleted() && !go.isNew()) {
    				go.getClassDef().getProvidedContext().getDataDeleteSink().deleteGeneric(go);
    			}
    		}
		}
		//flush sinks
        for (ZooClassDef cs: schemata) {
            cs.getProvidedContext().getDataDeleteSink().flush();
        }		

        //Then update. This matters for unique indices where deletion must occur before updates.
		for (ZooPCImpl co: cache.getDirtyObjects()) {
		    if (!co.jdoZooIsDirty()) {
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
		if (!cache.getDirtyGenericObjects().isEmpty()) {
			//TODO we are iterating twice through dirty/deleted objects... is that necessary?
    		for (GenericObject go: cache.getDirtyGenericObjects()) {
    			if (!go.isDeleted()) {
	    		    go.toStream();
	                go.getClassDef().getProvidedContext().getDataSink().writeGeneric(go);
    			}
    		}
		}
		
		//flush sinks
        for (ZooClassDef cs: schemata) {
            cs.getProvidedContext().getDataSink().flush();
        }		

		//delete schemata
		for (ZooClassDef cs: schemata) {
			if (cs.jdoZooIsDeleted() && !cs.jdoZooIsNew()) {
				cs.getProvidedContext().getNode().deleteSchema(cs);
			}
		}
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
		schema.associateFCOs(cachedSchemata);

//		TODO:
//			- construct fieldDefs here an give them to classDef.
//			- load required field type defs
//			- check cache (the cachedList only contains dirty/new schemata!)
	}


	public void rollback() {
		schemaManager.rollback();
		
		for (Node n: nodes) {
			n.rollback();
			//TODO two-phase rollback() ????
		}
		cache.rollback();
	}
	
	public void makePersistent(ZooPCImpl pc) {
		if (pc.jdoZooIsPersistent()) {
			if (pc.jdoZooGetPM() != pm) {
				throw new JDOUserException("The object belongs to a different persistence manager.");
			}
			if (pc.jdoZooIsDeleted()) {
				throw new JDOUserException("The object has been deleted!");
			}
			//nothing to do, is already persistent
			return; 
		}
		primary.makePersistent(pc);
	}

	public void makeTransient(ZooPCImpl pc) {
		if (!pc.jdoZooIsPersistent()) {
			//already transient
			return;
		}
		if (pc.jdoZooGetPM() != pm) {
			throw new JDOUserException("The object belongs to a different persistence manager.");
		}
		if (pc.jdoZooIsDirty()) {
			throw new JDOUserException("Dirty objects can not be made transient.");
		}
		//remove from cache
		cache.makeTransient((ZooPCImpl) pc);
	}

	public static void assertOid(long oid) {
		if (oid == OID_NOT_ASSIGNED) {
			throw new JDOFatalException("Invalid OID: " + oid);
		}
		
	}

	public static Session getSession(PersistenceManager pm) {
		return ((PersistenceManagerImpl) pm).getSession();
	}

	public static String oidToString(long oid) {
		String o1 = Long.toString(oid >> 48);
		String o2 = Long.toString((oid >> 32) & 0xFFFF);
		String o3 = Long.toString((oid >> 16) & 0xFFFF);
		String o4 = Long.toString(oid & 0xFFFF);
		return o1 + "." + o2 + "." + o3 + "." + o4 + ".";
	}
	
	public MergingIterator<ZooPCImpl> loadAllInstances(Class<?> cls, 
			boolean subClasses, 
            boolean loadFromCache) {
		MergingIterator<ZooPCImpl> iter = 
			new MergingIterator<ZooPCImpl>(this);
        ZooClassDef def = cache.getSchema(cls, primary);
		loadAllInstances(def.getVersionProxy(), subClasses, iter, loadFromCache);
		if (loadFromCache) {
			//also add 'new' instances
			iter.add(cache.iterator(def, subClasses, ObjectState.PERSISTENT_NEW));
		}
		return iter;
	}

	/**
	 * This method avoids nesting MergingIterators. 
	 * @param def
	 * @param subClasses
	 * @param iter
	 */
	private void loadAllInstances(ZooClassProxy def, boolean subClasses, 
			MergingIterator<ZooPCImpl> iter, boolean loadFromCache) {
		for (Node n: nodes) {
			iter.add(n.loadAllInstances(def, loadFromCache));
		}
		
		if (subClasses) {
			for (ZooClassProxy sub: def.getSubProxies()) {
				loadAllInstances(sub, true, iter, loadFromCache);
			}
		}
	}


	public ZooHandleImpl getHandle(long oid) {
		GenericObject gob = cache.getGeneric(oid);
		if (gob != null) {
			return gob.getOrCreateHandle();
		}
		
		ZooPCImpl co = cache.findCoByOID(oid);
        if (co != null) {
        	//GenericObject go = GenericObject.fromPCI(co);
        	ZooClassDef schema = co.jdoZooGetClassDef();
        	GenericObject go = co.jdoZooGetNode().readGenericObject(schema, oid);
        	return go.getOrCreateHandle();
        }

        try {
	        for (Node n: nodes) {
	        	//We should load the object only as byte[], if at all...
	        	ZooClassProxy schema = getSchemaManager().locateSchemaForObject(oid, n);
	        	GenericObject go = n.readGenericObject(schema.getSchemaDef(), oid);
	    		return go.getOrCreateHandle();
	        }
        } catch (JDOObjectNotFoundException e) {
        	//ignore, return null
        }
        return null;
	}

	public Object refreshObject(Object pc) {
        ZooPCImpl co = checkObject(pc);
        co.jdoZooGetNode().refreshObject(co);
        return pc;
	}
	
	/**
	 * Check for base class, persistence state and PM affiliation. 
	 * @param pc
	 * @return CachedObject
	 */
	private ZooPCImpl checkObject(Object pc) {
        if (!(pc instanceof ZooPCImpl)) {
        	throw new JDOUserException("The object is not persistent capable: " + pc.getClass());
        }
        
        ZooPCImpl pci = (ZooPCImpl) pc;
        if (!pci.jdoZooIsPersistent()) {
        	throw new JDOUserException("The object has not been made persistent yet.");
        }
        if (pci.jdoZooIsDeleted()) {
        	throw new JDOUserException("The object has alerady been deleted.");
        }

        if (pci.jdoZooGetPM() != pm) {
        	throw new JDOUserException("The object belongs to a different PersistenceManager.");
        }
        return pci;
	}


	public Object getObjectById(Object arg0) {
        long oid = (Long) arg0;
        ZooPCImpl co = cache.findCoByOID(oid);
        if (co != null) {
            if (co.jdoZooIsStateHollow() && !co.jdoZooIsDeleted()) {
                co.jdoZooGetNode().refreshObject(co);
            }
            return co;
        }

        //find it
        for (Node n: nodes) {
        	co = n.loadInstanceById(oid);
        	if (co != null) {
        		break;
        	}
        }

        return co;
	}
	
	public Object[] getObjectsById(Collection<? extends Object> arg0) {
		Object[] res = new Object[arg0.size()];
		int i = 0;
		for ( Object obj: arg0 ) {
			res[i] = getObjectById(obj);
			i++;
		}
		return res;
	}

	/**
	 * @param arg0
	 * @return Whether the object exists
	 */
	public boolean isOidUsed(long oid) {
        ZooPCImpl co = cache.findCoByOID(oid);
        if (co != null) {
        	return true;
        }
        //find it
        for (Node n: nodes) {
        	if (n.checkIfObjectExists(oid)) {
        		return true;
        	}
        }
        return false;
	}
	

	public void deletePersistent(Object pc) {
		ZooPCImpl co = checkObject(pc);
		co.jdoZooMarkDeleted();
	}


	public SchemaManager getSchemaManager() {
		return schemaManager;
	}


	public void close() {
		for (Node n: nodes) {
			n.closeConnection();
		}
		cache.close();
		TransientField.deregisterPm(pm);
	}


    public void refreshAll(Collection<?> arg0) {
		for ( Object obj: arg0 ) {
			refreshObject(obj);
		}
    }


    public PersistenceManagerImpl getPersistenceManager() {
        return pm;
    }


    public PersistenceManagerFactoryImpl getPersistenceManagerFactory() {
        return (PersistenceManagerFactoryImpl) pm.getPersistenceManagerFactory();
    }


    public void evictAll() {
        cache.evictAll();
    }


    public void evictAll(Object[] pcs) {
    	for (Object obj: pcs) {
    		ZooPCImpl pc = (ZooPCImpl) obj;
    		if (!pc.jdoZooIsDirty()) {
    			pc.jdoZooEvict();
    		}
    	}
    }


    public void evictAll(boolean subClasses, Class<?> cls) {
        cache.evictAll(subClasses, cls);
    }


	public Node getPrimaryNode() {
		return primary;
	}
	
	/**
	 * INTERNAL !!!!
	 * Iterators to be refreshed upon commit().
	 * @param it
	 */
	@Override
    public void registerIterator(CloseableIterator<?> it) {
        extents.put(it, null);
    }


    @Override
    public void deregisterIterator(CloseableIterator<?> iter) {
        extents.remove(iter);
    }


    public Collection<ZooPCImpl> getCachedObjects() {
        HashSet<ZooPCImpl> ret = new HashSet<ZooPCImpl>();
        for (ZooPCImpl o: cache.getAllObjects()) {
            ret.add(o);
        }
        return ret;
    }


    /**
     * Internal, don't call from outside!
     * @return The cache
     */
	public ClientSessionCache internalGetCache() {
		return cache;
	}


	public void addInstanceLifecycleListener(InstanceLifecycleListener listener,
			Class<?>[] classes) {
		for (Class<?> cls: classes) {
			ZooClassDef def = cache.getSchema(cls, primary);
			def.getProvidedContext().addLifecycleListener(listener);
		}
	}


	public void removeInstanceLifecycleListener(InstanceLifecycleListener listener) {
		for (ZooClassDef def: cache.getSchemata()) {
			def.getProvidedContext().removeLifecycleListener(listener);
		}
	}

}
