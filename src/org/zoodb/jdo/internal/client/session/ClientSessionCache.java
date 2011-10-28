package org.zoodb.jdo.internal.client.session;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;

import org.zoodb.jdo.internal.DataEvictor;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.client.CachedObject.CachedSchema;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.PrimLongMapLI;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class ClientSessionCache implements AbstractCache {
	
	//Do not use list to indicate properties! Instead of 1 bit it, lists require 20-30 bytes per entry!
	//ArrayList is better than ObjIdentitySet, because the latter does not support Iterator.remove
	//ArrayList may allocate to large of an array! Implement BucketedList instead ! TODO!
	//Also: ArrayList.remove is expensive!! TODO
	//TODO Optimize PrimLongTreeMap further? -> HashMaps don't scale!!! (because of the internal array)
	//private HashMap<Long, CachedObject> _objs = new HashMap<Long,CachedObject>();
    private final PrimLongMapLI<CachedObject> objs = new PrimLongMapLI<CachedObject>();
	
	private final PrimLongMapLI<CachedObject.CachedSchema> schemata = 
		new PrimLongMapLI<CachedObject.CachedSchema>();
	//TODO move into node-cache
	private final HashMap<Node, HashMap<Class<?>, CachedObject.CachedSchema>> nodeSchemata = 
		new HashMap<Node, HashMap<Class<?>, CachedSchema>>();
	
	private final Session session;
	
	public ClientSessionCache(Session session) {
		this.session = session;
	}
	
	public Session getSession() {
		return session;
	}
	
	public boolean isSchemaDefined(Class<?> type, Node node) {
		return (getCachedSchema(type, node) != null);
	}


	public void rollback() {
		//TODO refresh cleans?  may have changed in DB?
		//Maybe set them all to hollow instead? //TODO

	    //refresh schemata
        //Reloading needs to be in a separate loop. We first need to remove all from the cache
        //before reloading them. Reloading may implicitly load dirty super-classes, which would
        //fail if they are still in the cache and marked as dirty.
	    LinkedList<CachedSchema> schemaToRefresh = new LinkedList<CachedObject.CachedSchema>();
        for (CachedSchema cs: schemata.values()) {
        	if (cs.isDirty()) {
        		schemata.remove(cs.getOID());
        		nodeSchemata.get(cs.getNode()).remove(cs.getSchema().getJavaClass());
        		schemaToRefresh.add(cs);
        	}
        }
        for (CachedSchema cs: schemaToRefresh) {
            session.getSchemaManager().locateSchema(cs.getSchema().getJavaClass(), cs.getNode());
        }
        
	    //TODO Maybe we should simply refresh the whole cache instead of setting them to hollow.
        //This doesn't matter for embedded databases, but for client/server, we could benefit from
        //group-refreshing(loading) all dirty objects 
	    Iterator<CachedObject> iter = objs.values().iterator();
	    while (iter.hasNext()) {
	    	CachedObject co = iter.next();
	    	if (co.isDirty()) {
	    		if (co.isNew()) {
	    			//remove co
	    			iter.remove();
	    		} else {
	    			co.markHollow();
	    		}
	    	}
	    }
	}


	public void markPersistent(PersistenceCapableImpl pc, long oid, Node node) {
		if (pc.jdoIsDeleted()) {
			throw new UnsupportedOperationException("Make it persistent again");
			//TODO implement
		}
		if (pc.jdoIsPersistent()) {
			//ignore
			return;
		}
		ZooClassDef classDef = getSchema(pc.getClass(), node);
		CachedObject co = new CachedObject(pc, ObjectState.PERSISTENT_NEW, classDef.getBundle());
		//TODO call newInstance elsewhere
		//obj.jdoReplaceStateManager(co);
		pc.jdoNewInstance(co);
		pc.jdoZooSetOid(oid);
		objs.put(oid, co);
	}


	public void makeTransient(PersistenceCapableImpl pc) {
		//first check
		CachedObject co = objs.get(pc.jdoZooGetOid());
		if (co == null) {
			throw new JDOFatalDataStoreException("Object is not in cache.");
		}
		//update
		co.markTransient();
		//only now we can remove it
		objs.remove(pc.jdoZooGetOid());
	}


	public void addHollow(PersistenceCapableImpl obj, Node node, ZooClassDef classDef, long oid) {
    	CachedObject co = new CachedObject(obj, ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL,  
				classDef.getBundle());
		//TODO call newInstance elsewhere
		//obj.jdoReplaceStateManager(co);
		obj.jdoNewInstance(co);
		objs.put(co.getOID(), co);
	}

	public void addPC(PersistenceCapableImpl obj, Node node, ZooClassDef classDef, long oid) {
    	CachedObject co = new CachedObject(obj, ObjectState.PERSISTENT_CLEAN, classDef.getBundle());
		//TODO call newInstance elsewhere
		//obj.jdoReplaceStateManager(co);
		obj.jdoNewInstance(co);
		objs.put(co.getOID(), co);
	}

	@Override
	public final CachedObject findCoByOID(long oid) {
		return objs.get(oid);
	}

	/**
	 * TODO Fix this. Schemata should be kept in a separate cache
	 * for each node!
	 * @param def
	 * @param node
	 * @return 
	 */
	public CachedSchema getCachedSchema(Class<?> cls, Node node) {
		return nodeSchemata.get(node).get(cls);
	}

	public ZooClassDef getSchema(Class<?> cls, Node node) {
		CachedSchema cs = nodeSchemata.get(node).get(cls);
		if (cs != null) {
			return cs.getSchema();
		}
		return null;
	}

	@Override
	public ZooClassDef getSchema(long schemaOid) {
		return schemata.get(schemaOid).getSchema();
	}

	/**
	 * Clean out the cache after commit.
	 * TODO keep hollow objects? E.g. references to correct, e.t.c!
	 */
	public void postCommit() {
		//TODO later: empty cache (?)
		Iterator<CachedObject> iter = objs.values().iterator();
		for (; iter.hasNext(); ) {
			CachedObject co = iter.next();
			if (co.isDeleted()) {
				iter.remove();
				continue;
			}
            DataEvictor.nullify(co);
			co.markHollow();
			//TODO WHy clean? removes the hollow-flag
//			co.markClean();  //TODO remove if cache is flushed -> retainValues!!!!!
//			co.obj = null;
			//TODO set all fields to null;
		}
		Iterator<CachedSchema> iterS = schemata.values().iterator();
		for (; iterS.hasNext(); ) {
			CachedSchema cs = iterS.next();
			if (cs.isDeleted()) {
				iterS.remove();
        		nodeSchemata.get(cs.getNode()).remove(cs.getSchema().getJavaClass());
				continue;
			}
			//TODO keep in cache???
			cs.markHollow();
			cs.markClean();  //TODO remove if cache is flushed -> retainValues!!!!!
		}
	}

	/**
	 * 
	 * @param node
	 * @return List of all cached schema objects for that node (clean, new, deleted, dirty).
	 */
	public Collection<CachedObject.CachedSchema> getSchemata(Node node) {
		return nodeSchemata.get(node).values();
	}
	
	public void addSchema(ZooClassDef clsDef, boolean isLoaded, Node node) {
		ObjectState state = isLoaded ? ObjectState.PERSISTENT_CLEAN : ObjectState.PERSISTENT_NEW;
		clsDef.setBundle(session, node);
		CachedObject.CachedSchema cs = new CachedObject.CachedSchema(clsDef, state);
		schemata.put(clsDef.getOid(), cs);
		nodeSchemata.get(node).put(clsDef.getJavaClass(), cs);
	}

	public Collection<CachedObject> getAllObjects() {
		return Collections.unmodifiableCollection(objs.values());
	}

    public void close() {
        objs.clear();
        schemata.clear();
        nodeSchemata.clear();
    }


    public void evictAll() {
//        objs.clear();
        for (CachedObject co: objs.values()) {
            if (!co.isDirty()) {
                DataEvictor.nullify(co);
                co.markHollow();
            }
        }

//        Iterator<CachedObject> it = objs.values().iterator();
//        while (it.hasNext()) {
//            CachedObject co = it.next();
//            if (!co.isDirty()) {
//                it.remove();
//            }
//        }
    }

    public void evictAll(Object[] pcs) {
        for (Object obj: pcs) {
            PersistenceCapableImpl pc = (PersistenceCapableImpl) obj;
            CachedObject co = pc.jdoZooGetStateManager(); 
            if (!co.isDirty()) {
                DataEvictor.nullify(co);
                co.markHollow();
                //objs.remove(oid);
            }
        }
    }


    public void evictAll(boolean subClasses, Class<?> cls) {
        //TODO use special high-perf iterators?
//        Iterator<CachedObject> it = objs.values().iterator();
//        while (it.hasNext()) {
//            CachedObject co = it.next();
        for (CachedObject co: objs.values()) {
            if (!co.isDirty() && (co.getClassDef().getJavaClass() == cls || 
                    (subClasses && cls.isAssignableFrom(co.getClassDef().getJavaClass())))) {
                //it.remove();
                DataEvictor.nullify(co);
                co.markHollow();
            }
        }
    }

	public void addNode(Node node) {
		nodeSchemata.put(node, new HashMap<Class<?>, CachedObject.CachedSchema>());
	}

	public CloseableIterator<PersistenceCapableImpl> iterator(ZooClassDef cls, boolean subClasses, 
			ObjectState state) {
		return new CacheIterator(objs.values().iterator(), cls, subClasses, state);
	}
	
	
	private static class CacheIterator implements CloseableIterator<PersistenceCapableImpl> {

		private PersistenceCapableImpl next = null;
		private final Iterator<CachedObject> iter;
		private final ZooClassDef cls;
		private final boolean subClasses;
		private final ObjectState state;
		
		private CacheIterator(Iterator<CachedObject> iter, ZooClassDef cls, boolean subClasses, 
				ObjectState state) {
			this.iter = iter;
			this.cls = cls;
			this.subClasses = subClasses;
			this.state = state;
			//find first object
			next();
		}
		
		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public PersistenceCapableImpl next() {
			PersistenceCapableImpl ret = next;
			CachedObject co = null;
			while (iter.hasNext()) {
				co = iter.next();
				ZooClassDef defCand = co.getClassDef();
				if (defCand == cls || (subClasses && cls.hasSuperClass(cls))) {
					if (co.hasState(state)) {
						next = co.obj;
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
}
