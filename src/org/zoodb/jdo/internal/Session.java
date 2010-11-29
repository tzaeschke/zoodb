package org.zoodb.jdo.internal;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.jdo.JDOFatalException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.zoodb.jdo.PersistenceManagerImpl;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.client.SchemaManager;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.spi.StateManagerImpl;

public class Session {//implements TxAPI {

	public static final long OID_NOT_ASSIGNED = 0;
	public static final long OID_INVALID = -1;

	public static final Class PERSISTENT_SUPER = Object.class;
	
	/** Primary node. Also included in the _nodes list. */
	private Node _primary;
	/** All connected nodes. Includes the primary node. */
	private List<Node> _nodes = new LinkedList<Node>();
	private PersistenceManagerImpl _pm;
	private ClientSessionCache _cache;
	private SchemaManager _schemaManager;
	private StateManagerImpl _sm;
	
	public Session(PersistenceManagerImpl pm, String nodePath) {
		_pm = pm;
		_sm = new StateManagerImpl(this);
		_cache = new ClientSessionCache(this, _sm);
		_sm.setCache(_cache);
		_schemaManager = new SchemaManager(_cache);
		_primary = ZooFactory.get().createNode(nodePath, _cache);
		_nodes.add(_primary);
	}
	
	
	public void commit(boolean retainValues) {
		//pre-commit: traverse object tree for transitive persistence
		ObjectGraphTraverser ogt = new ObjectGraphTraverser(_pm, _cache);
		ogt.traverse();
		
		
		for (Node n: _nodes) {
			n.commit();
			//TODO two-phase commit() !!!
		}
		System.err.println("FIXME: 2-phase Session.commit()");
	}

	public void rollback() {
		for (Node n: _nodes) {
			n.rollback();
			//TODO two-phase rollback() ????
		}
		_cache.rollback();
	}
	
	public void makePersistent(PersistenceCapableImpl obj) {
		_primary.makePersistent(obj);
	}

	public void makeTransient(Object pc) {
		System.err.println("STUB: Connection.makeTransient()");
		//remove from cache
		//_cache.makeTransient(pc);
		throw new UnsupportedOperationException();
	}

	public static void assertOid(long oid) {
		if (oid == OID_NOT_ASSIGNED || oid == OID_INVALID) {
			throw new JDOFatalException("Invalid OID: " + oid);
		}
		
	}

	public static Session getSession(PersistenceManager pm) {
		return ((PersistenceManagerImpl) pm).getSession();
	}

	public Node getNode(String nodeName) {
		for (Node n: _nodes) {
			if (n.getURL().equals(nodeName)) {
				return n;
			}
		}
		//TODO remove
		if (true) {
			for (Node n: _nodes) {
				System.err.println("Node: " + n.getURL() + " / " + n.getDbPath());
			}
			throw new RuntimeException("Node not found: " + nodeName);
		}
		return null;
	}


	public static String oidToString(long oid) {
		String o1 = Long.toString(oid >> 48);
		String o2 = Long.toString((oid >> 32) & 0xFFFF);
		String o3 = Long.toString((oid >> 16) & 0xFFFF);
		String o4 = Long.toString(oid & 0xFFFF);
		return o1 + "." + o2 + "." + o3 + "." + o4 + ".";
	}
	
	public List loadAllInstances(Class cls, boolean subclasses, Class minClass) {
		//TODO implement merging iterator...
		ArrayList all = new ArrayList();
		for (Node n: _nodes) {
			all.addAll(n.loadAllInstances(cls));
		}
		Class sup = cls.getSuperclass();
		if (subclasses && sup != PERSISTENT_SUPER && minClass.isAssignableFrom(sup)) {
			all.addAll(loadAllInstances(sup, true, minClass));
		}
		return all;
	}


	public Object[] getObjectsById(Object ... arg0) {
		Object[] res = new Object[arg0.length];
		for ( int i = 0; i < arg0.length; i++ ) {
			long oid = (Long) arg0[i];
			PersistenceCapableImpl o = null;
			CachedObject co = _cache.findCoByOID(oid);
			if (co != null) {
				o = co.getObject();
				if (co.isStateHollow()) {
					o = co.getNode().loadInstanceById(co.getOID());
				}
			}
			if (o == null) {
				for (Node n: _nodes) {
					o = n.loadInstanceById(oid);
					if (o != null) {
						break;
					}
				}
			}
			if (o == null) {
				//TODO how should this be in JDO?
				throw new JDOObjectNotFoundException("OID=" + Util.oidToString(oid));
			}
			res[i] = o;
		}
		return res;
	}


	public void deletePersistent(Object pc) {
        if (!(pc instanceof PersistenceCapableImpl)) {
        	throw new JDOUserException("Object is not persistent capable: " + 
        			pc.getClass());
        }
		PersistenceCapableImpl pci = (PersistenceCapableImpl) pc;
		CachedObject co = _cache.findCO(pci);
		if (co == null) {
			System.out.println("Trying to load: " + pci.jdoGetObjectId());
			getObjectsById(pci.jdoGetObjectId());
			co = _cache.findCO(pci);
		}
        _cache.deletePersistent(co);
	}


	public SchemaManager getSchemaManager() {
		return _schemaManager;
	}


	public void close() {
		for (Node n: _nodes) {
			n.closeConnection();
		}
	}
}
