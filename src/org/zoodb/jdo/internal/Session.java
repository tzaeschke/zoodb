package org.zoodb.jdo.internal;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.JDOFatalException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;

import org.zoodb.jdo.PersistenceManagerFactoryImpl;
import org.zoodb.jdo.PersistenceManagerImpl;
import org.zoodb.jdo.internal.client.SchemaManager;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.internal.util.DatabaseLogger;
import org.zoodb.jdo.internal.util.MergingIterator;
import org.zoodb.jdo.internal.util.TransientField;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class Session {

	public static final long OID_NOT_ASSIGNED = -1;

	public static final Class<?> PERSISTENT_SUPER = PersistenceCapableImpl.class;
	
	/** Primary node. Also included in the _nodes list. */
	private Node primary;
	/** All connected nodes. Includes the primary node. */
	private final List<Node> nodes = new LinkedList<Node>();
	private final PersistenceManagerImpl pm;
	private final ClientSessionCache cache;
	private final SchemaManager schemaManager;
	
	public Session(PersistenceManagerImpl pm, String nodePath) {
		this.pm = pm;
		this.cache = new ClientSessionCache(this);
		this.schemaManager = new SchemaManager(cache);
		this.primary = ZooFactory.get().createNode(nodePath, cache);
		this.nodes.add(primary);
		this.cache.addNode(primary);
		this.primary.connect();
	}
	
	
	public void commit(boolean retainValues) {
		//pre-commit: traverse object tree for transitive persistence
		ObjectGraphTraverser ogt = new ObjectGraphTraverser(pm, cache);
		ogt.traverse();
		
		schemaManager.commit();
		
		for (Node n: nodes) {
			n.commit();
			//TODO two-phase commit() !!!
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
	
	public void makePersistent(PersistenceCapableImpl pc) {
		if (pc.jdoZooIsPersistent()) {
			if (pc.jdoZooGetPM() != pm) {
				throw new JDOUserException("The object belongs to a different persistence manager.");
			}
			//nothing to do, is already persistent
			return; 
		}
		primary.makePersistent(pc);
	}

	public void makeTransient(PersistenceCapableImpl pc) {
		if (!pc.jdoZooIsPersistent()) {
			//already transient
			return;
		}
		if (pc.jdoZooGetPM() != pm) {
			throw new JDOUserException("The object belongs to a different persistence manager.");
		}
		System.err.println("STUB: Connection.makeTransient()");
		//remove from cache
		cache.makeTransient((PersistenceCapableImpl) pc);
	}

	public static void assertOid(long oid) {
		if (oid == OID_NOT_ASSIGNED) {
			throw new JDOFatalException("Invalid OID: " + oid);
		}
		
	}

	public static Session getSession(PersistenceManager pm) {
		return ((PersistenceManagerImpl) pm).getSession();
	}

	public Node getNode(String nodeName) {
		for (Node n: nodes) {
			if (n.getURL().equals(nodeName)) {
				return n;
			}
		}
		throw new RuntimeException("Node not found: " + nodeName);
	}


	public static String oidToString(long oid) {
		String o1 = Long.toString(oid >> 48);
		String o2 = Long.toString((oid >> 32) & 0xFFFF);
		String o3 = Long.toString((oid >> 16) & 0xFFFF);
		String o4 = Long.toString(oid & 0xFFFF);
		return o1 + "." + o2 + "." + o3 + "." + o4 + ".";
	}
	
	public MergingIterator<PersistenceCapableImpl> loadAllInstances(Class<?> cls, 
			boolean subClasses, 
            boolean loadFromCache) {
		MergingIterator<PersistenceCapableImpl> iter = 
			new MergingIterator<PersistenceCapableImpl>();
		loadAllInstances(cls, subClasses, iter, loadFromCache);
		if (loadFromCache) {
			//also add 'new' instances
			ZooClassDef def = cache.getCachedSchema(cls, primary).getSchema();
			iter.add(cache.iterator(def, subClasses, ObjectState.PERSISTENT_NEW));
		}
		return iter;
	}

	/**
	 * This method avoids nesting MergingIterators. 
	 * @param cls
	 * @param subClasses
	 * @param iter
	 */
	private void loadAllInstances(Class<?> cls, boolean subClasses, 
			MergingIterator<PersistenceCapableImpl> iter, 
            boolean loadFromCache) {
		ZooClassDef def = cache.getSchema(cls, primary);
		for (Node n: nodes) {
			iter.add(n.loadAllInstances(def, loadFromCache));
		}
		
		if (subClasses) {
			Collection<ZooClassDef> subs = def.getSubClasses();
			for (ZooClassDef sub: subs) {
				loadAllInstances(sub.getJavaClass(), true, iter, loadFromCache);
			}
		}
	}


	public ZooHandle getHandle(long oid) {
		PersistenceCapableImpl co = cache.findCoByOID(oid);
        if (co != null) {
        	System.err.println("FIXME get directly from co.getClassDef()");
        	ISchema schema = getSchemaManager().locateSchema(co.getClass(), co.jdoZooGetNode());
        	return new ZooHandle(oid, co.jdoZooGetNode(), this, schema);
        }

        for (Node n: nodes) {
        	//TODO uh, this is bad. We should load the object only as byte[], if at all
        	System.out.println("FIXME: Session.getHandle");
        	Object o = n.loadInstanceById(oid);
        	if (o != null) {
            	ISchema schema = getSchemaManager().locateSchema(o.getClass(), n);
        		return new ZooHandle(oid, n, this, schema);
        	}
        }

        throw new JDOObjectNotFoundException("OID=" + Util.oidToString(oid));
	}

	public Object refreshObject(Object pc) {
        PersistenceCapableImpl co = checkObject(pc);
        co.jdoZooGetNode().refreshObject(co);
        return pc;
	}
	
	/**
	 * Check for base class, persistence state and PM affiliation. 
	 * @param pc
	 * @return CachedObject
	 */
	private PersistenceCapableImpl checkObject(Object pc) {
        if (!(pc instanceof PersistenceCapableImpl)) {
        	throw new JDOUserException("The object is not persistent capable: " + pc.getClass());
        }
        
        PersistenceCapableImpl pci = (PersistenceCapableImpl) pc;
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
        PersistenceCapableImpl co = cache.findCoByOID(oid);
        if (co != null) {
            if (co.jdoZooIsStateHollow()) {
                co.jdoZooGetNode().loadInstanceById(oid);
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
		PersistenceCapableImpl co = checkObject(pc);
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
    		PersistenceCapableImpl pc = (PersistenceCapableImpl) obj;
    		if (!pc.jdoZooIsDirty()) {
    			DataEvictor.nullify(pc);
    			pc.jdoZooMarkHollow();
    		}
    	}
    }


    public void evictAll(boolean subClasses, Class<?> cls) {
        cache.evictAll(subClasses, cls);
    }


	public Node getPrimaryNode() {
		return primary;
	}
}
