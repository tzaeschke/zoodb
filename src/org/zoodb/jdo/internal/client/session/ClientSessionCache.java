/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal.client.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.ObjectState;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.PrimLongMapLI;

public class ClientSessionCache implements AbstractCache {
	
	//Do not use list to indicate properties! Instead of 1 bit it, lists require 20-30 bytes per entry!
	//ArrayList is better than ObjIdentitySet, because the latter does not support Iterator.remove
	//ArrayList may allocate to large of an array! Implement BucketedList instead ! TODO!
	//Also: ArrayList.remove is expensive!! TODO
	//TODO Optimize PrimLongTreeMap further? -> HashMaps don't scale!!! (because of the internal array)
	//private HashMap<Long, CachedObject> _objs = new HashMap<Long,CachedObject>();
    private final PrimLongMapLI<ZooPCImpl> objs = 
    	new PrimLongMapLI<ZooPCImpl>();
	
	private final PrimLongMapLI<ZooClassDef> schemata = 
		new PrimLongMapLI<ZooClassDef>();
	//TODO move into node-cache
	private final HashMap<Node, HashMap<Class<?>, ZooClassDef>> nodeSchemata = 
		new HashMap<Node, HashMap<Class<?>, ZooClassDef>>();
	
	private final Session session;
	
	public ClientSessionCache(Session session) {
		this.session = session;
	}
	
	public Session getSession() {
		return session;
	}
	
	public boolean isSchemaDefined(Class<?> type, Node node) {
		return (getSchema(type, node) != null);
	}


	public void rollback() {
		//TODO refresh cleans?  may have changed in DB?
		//Maybe set them all to hollow instead? //TODO

	    //refresh schemata
        //Reloading needs to be in a separate loop. We first need to remove all from the cache
        //before reloading them. Reloading may implicitly load dirty super-classes, which would
        //fail if they are still in the cache and marked as dirty.
	    ArrayList<ZooClassDef> schemaToRefresh = new ArrayList<ZooClassDef>();
        for (ZooClassDef cs: schemata.values()) {
        	if (cs.jdoZooIsDirty()) {
        		if (cs.jdoZooIsNew()) {
        			schemata.remove(cs.jdoZooGetOid());
        			nodeSchemata.get(cs.jdoZooGetNode()).remove(cs.getJavaClass());
        		} else {
        			schemaToRefresh.add(cs);
        		}
        	}
        	
        }
        for (ZooClassDef cs: schemaToRefresh) {
       		session.getSchemaManager().refreshSchema(cs);
        }
        
	    //TODO Maybe we should simply refresh the whole cache instead of setting them to hollow.
        //This doesn't matter for embedded databases, but for client/server, we could benefit from
        //group-refreshing(loading) all dirty objects 
	    Iterator<ZooPCImpl> iter = objs.values().iterator();
	    while (iter.hasNext()) {
	    	ZooPCImpl co = iter.next();
	    	if (co.jdoZooIsDirty()) {
	    		if (co.jdoZooIsNew()) {
	    			//remove co
	    			iter.remove();
	    		} else {
	    			co.jdoZooMarkHollow();
	    		}
	    	}
	    }
	}


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
			throw new JDOFatalDataStoreException("Object is not in cache.");
		}
		//update
		pc.jdoZooMarkTransient();
	}


	public final void addToCache(ZooPCImpl obj, ZooClassDef classDef, long oid, 
			ObjectState state) {
    	obj.jdoZooInit(state, classDef.jdoZooGetContext(), oid);
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
	 * @param def
	 * @param node
	 * @return 
	 */
	public ZooClassDef getSchema(Class<?> cls, Node node) {
		return nodeSchemata.get(node).get(cls);
	}

	public ZooClassDef getSchema(String clsName, Node node) {
		for (ZooClassDef def: schemata.values()) {
			if (def.getClassName().equals(clsName)) {
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
	public void postCommit() {
		final boolean retainValues = session.getPersistenceManagerFactory().getRetainValues();
		//TODO later: empty cache (?)
		PrimLongMapLI<ZooPCImpl>.ValueIterator iter = objs.values().iterator();
		for (; iter.hasNext(); ) {
			ZooPCImpl co = iter.next();
			if (co.jdoZooIsDeleted()) {
				iter.remove();
				continue;
			}
			if (retainValues) {
				co.jdoZooMarkClean();
			} else {
				co.jdoZooEvict();
			}
		}
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
	 * 
	 * @param node
	 * @return List of all cached schema objects for that node (clean, new, deleted, dirty).
	 */
	public Collection<ZooClassDef> getSchemata(Node node) {
		return nodeSchemata.get(node).values();
	}
	
	public void addSchema(ZooClassDef clsDef, boolean isLoaded, Node node) {
		ObjectState state;
		if (isLoaded) {
			state = ObjectState.PERSISTENT_CLEAN;
		} else {
			state = ObjectState.PERSISTENT_NEW;
		}
		clsDef.initPersCapable(state, session, node);
		schemata.put(clsDef.getOid(), clsDef);
		nodeSchemata.get(node).put(clsDef.getJavaClass(), clsDef);
	}
	
	public void updateSchema(ZooClassDef clsDef, Class<?> oldCls, Class<?> newCls) {
		Node node = clsDef.jdoZooGetNode();
		//Removal may return null if class was previously stored a 'null', which is non-unique.
		nodeSchemata.get(node).remove(oldCls);
		if (newCls != null) {
			nodeSchemata.get(node).put(newCls, clsDef);
		}
	}

	public PrimLongMapLI<ZooPCImpl>.PrimLongValues getAllObjects() {
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
	}

	public CloseableIterator<ZooPCImpl> iterator(ZooClassDef cls, boolean subClasses, 
			ObjectState state) {
		return new CacheIterator(objs.values().iterator(), cls, subClasses, state);
	}
	
	
	private static class CacheIterator implements CloseableIterator<ZooPCImpl> {

		private ZooPCImpl next = null;
		private final PrimLongMapLI<ZooPCImpl>.ValueIterator iter;
		private final ZooClassDef cls;
		private final boolean subClasses;
		private final ObjectState state;
		
		private CacheIterator(PrimLongMapLI<ZooPCImpl>.ValueIterator iter, 
				ZooClassDef cls, boolean subClasses, ObjectState state) {
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
}
