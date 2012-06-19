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
package org.zoodb.jdo.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.WeakHashMap;

import javax.jdo.JDOFatalException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.PersistenceManagerFactoryImpl;
import org.zoodb.jdo.PersistenceManagerImpl;
import org.zoodb.jdo.internal.client.SchemaManager;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.DatabaseLogger;
import org.zoodb.jdo.internal.util.MergingIterator;
import org.zoodb.jdo.internal.util.TransientField;
import org.zoodb.jdo.internal.util.Util;

public class Session {

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
			for (Node n: nodes) {
				n.commit();
				//TODO two-phase commit() !!!
			}
		} catch (JDOUserException e) {
			//allow for retry after user exceptions
			for (Node n: nodes) {
				n.revert();
				//TODO two-phase commit() !!!
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
		DatabaseLogger.debugPrintln(2, "FIXME: 2-phase Session.commit()");
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
			new MergingIterator<ZooPCImpl>();
        ZooClassDef def = cache.getSchema(cls, primary);
		loadAllInstances(def, subClasses, iter, loadFromCache);
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
	private void loadAllInstances(ZooClassDef def, boolean subClasses, 
			MergingIterator<ZooPCImpl> iter, 
            boolean loadFromCache) {
		for (Node n: nodes) {
			iter.add(n.loadAllInstances(def, loadFromCache));
		}
		
		if (subClasses) {
			Collection<ZooClassDef> subs = def.getSubClasses();
			for (ZooClassDef sub: subs) {
				loadAllInstances(sub, true, iter, loadFromCache);
			}
		}
	}


	public ZooHandle getHandle(long oid) {
		ZooPCImpl co = cache.findCoByOID(oid);
        if (co != null) {
        	ISchema schema = co.jdoZooGetClassDef().getApiHandle();
        	return new ZooHandle(oid, co.jdoZooGetNode(), this, schema);
        }

        for (Node n: nodes) {
        	System.out.println("FIXME: Session.getHandle");
        	//We should load the object only as byte[], if at all...
        	ISchema schema = getSchemaManager().locateSchemaForObject(oid, n);
    		return new ZooHandle(oid, n, this, schema);
        	
//        	//TODO uh, this is bad. We should load the object only as byte[], if at all
//        	Object o = n.loadInstanceById(oid);
//        	if (o != null) {
//            	ISchema schema = getSchemaManager().locateSchema(o.getClass(), n);
//        		return new ZooHandle(oid, n, this, schema);
//        	}
        }

        throw new JDOObjectNotFoundException("OID=" + Util.oidToString(oid));
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

        if (co == null) {
            //TODO how should this be in JDO?
            throw new JDOObjectNotFoundException("OID=" + Util.oidToString(oid));
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
	 * @param it
	 */
    public void registerExtentIterator(CloseableIterator<?> it) {
        extents.put(it, null);
    }

}
