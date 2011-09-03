package org.zoodb.jdo.internal;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.jdo.JDOFatalException;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.zoodb.jdo.PersistenceManagerFactoryImpl;
import org.zoodb.jdo.PersistenceManagerImpl;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.client.SchemaManager;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.DatabaseLogger;
import org.zoodb.jdo.stuff.MergingIterator;
import org.zoodb.jdo.stuff.TransientField;

public class Session {

	public static final long OID_NOT_ASSIGNED = -1;

	public static final Class<?> PERSISTENT_SUPER = PersistenceCapableImpl.class;
	
	/** Primary node. Also included in the _nodes list. */
	private Node _primary;
	/** All connected nodes. Includes the primary node. */
	private final List<Node> _nodes = new LinkedList<Node>();
	private final PersistenceManagerImpl _pm;
	private final ClientSessionCache _cache;
	private final SchemaManager _schemaManager;
	
	public Session(PersistenceManagerImpl pm, String nodePath) {
		_pm = pm;
		_cache = new ClientSessionCache(this);
		_schemaManager = new SchemaManager(_cache);
		_primary = ZooFactory.get().createNode(nodePath, _cache);
		_nodes.add(_primary);
		_cache.addNode(_primary);
		_primary.connect();
	}
	
	
	public void commit(boolean retainValues) {
		//pre-commit: traverse object tree for transitive persistence
		ObjectGraphTraverser ogt = new ObjectGraphTraverser(_pm, _cache);
		ogt.traverse();
		
		_schemaManager.commit();
		
		for (Node n: _nodes) {
			n.commit();
			//TODO two-phase commit() !!!
		}
		DatabaseLogger.debugPrintln(2, "FIXME: 2-phase Session.commit()");
	}

	public void rollback() {
		_schemaManager.rollback();
		
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
		if (oid == OID_NOT_ASSIGNED) {
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
			boolean subClasses) {
		MergingIterator<PersistenceCapableImpl> iter = 
			new MergingIterator<PersistenceCapableImpl>();
		loadAllInstances(cls, subClasses, iter);
		return iter;
	}

	/**
	 * This method avoids nesting MergingIterators. 
	 * @param cls
	 * @param subClasses
	 * @param iter
	 */
	private void loadAllInstances(Class<?> cls, boolean subClasses, 
			MergingIterator<PersistenceCapableImpl> iter) {
		ZooClassDef def = _cache.getSchema(cls, _primary);
		for (Node n: _nodes) {
			iter.add(n.loadAllInstances(def));
		}
		
		if (subClasses) {
			Set<ZooClassDef> subs = def.getSubClasses();
			for (ZooClassDef sub: subs) {
				loadAllInstances(sub.getJavaClass(), true, iter);
			}
		}
	}


	public ZooHandle getHandle(long oid) {
        CachedObject co = _cache.findCoByOID(oid);
        if (co != null) {
        	ISchema schema = getSchemaManager().locateSchema(co.obj.getClass(), co.node);
        	return new ZooHandle(oid, co.node, this, schema);
        }

        for (Node n: _nodes) {
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

	public Object refreshObjectById(Object arg0) {
        long oid = (Long) arg0;
        PersistenceCapableImpl o = null;
        CachedObject co = _cache.findCoByOID(oid);
        if (co != null) {
        	//TODO can we be sure that the node is still correct and has not changed?
        	o = co.getNode().loadInstanceById(oid);
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
        return o;
	}
	
	public Object getObjectById(Object arg0) {
        long oid = (Long) arg0;
        PersistenceCapableImpl o = null;
        CachedObject co = _cache.findCoByOID(oid);
        if (co != null) {
            o = co.getObject();
            if (co.isStateHollow()) {
                o = co.getNode().loadInstanceById(oid);
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
        return o;
	}
	
	public Object[] getObjectsById(Collection<? extends Object> arg0) {
		Object[] res = new Object[arg0.size()];
		int i = 0;
		for ( Object obj: arg0 ) {
			long oid = (Long) obj;
			PersistenceCapableImpl o = null;
			CachedObject co = _cache.findCoByOID(oid);
			if (co != null) {
				o = co.getObject();
				if (co.isStateHollow()) {
					o = co.getNode().loadInstanceById(oid);
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
			i++;
		}
		return res;
	}


	public void deletePersistent(Object pc) {
        if (!(pc instanceof PersistenceCapableImpl)) {
        	throw new JDOUserException("Object is not persistent capable: " + pc.getClass());
        }
		PersistenceCapableImpl pci = (PersistenceCapableImpl) pc;
		((CachedObject)pci.jdoZooGetStateManager()).markDeleted();
	}


	public SchemaManager getSchemaManager() {
		return _schemaManager;
	}


	public void close() {
		for (Node n: _nodes) {
			n.closeConnection();
		}
		_cache.close();
		TransientField.deregisterPm(_pm);
	}


    public void refreshAll(Collection<?> arg0) {
		for ( Object obj: arg0 ) {
			if (!(obj instanceof PersistenceCapableImpl)) {
				throw new JDOUserException("Can not refresh non-persistent capable object.");
			}
			PersistenceCapableImpl pc = (PersistenceCapableImpl) obj;

			if (!pc.jdoIsPersistent()) {
				//skip
				continue;
			}
			CachedObject co = (CachedObject) pc.jdoZooGetStateManager();
			if (co != null) {
				pc = co.getNode().loadInstanceById(co.getOID());
			} else {
				throw new JDOObjectNotFoundException("OID=" + 
						Util.oidToString(pc.jdoGetObjectId()));
				//TODO not cached object? Do we really need to cover this?
				//-> It would mean that the object is in memory, but has not been loaded!?!?!
//				for (Node n: _nodes) {
//					o = n.loadInstanceById(oid);
//					if (o != null) {
//						break;
//					}
//				}
			}
		}
    }


    public PersistenceManagerImpl getPersistenceManager() {
        return _pm;
    }


    public PersistenceManagerFactoryImpl getPersistenceManagerFactory() {
        return (PersistenceManagerFactoryImpl) _pm.getPersistenceManagerFactory();
    }


    public void evictAll(Object[] pcs) {
        _cache.evictAll(pcs);
    }


	public Node getPrimaryNode() {
		return _primary;
	}
}
