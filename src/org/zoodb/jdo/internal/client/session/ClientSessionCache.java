package org.zoodb.jdo.internal.client.session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import javax.jdo.ObjectState;
import javax.jdo.spi.StateManager;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.client.CachedObject.CachedSchema;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.spi.StateManagerImpl;

public class ClientSessionCache extends AbstractCache {
	
	//Do not use list to indicate properties! Instead of 1 bit it, lists require 20-30 bytes per entry!
	//ArrayList is better than ObjIdentitySet, because the latter does not support Iterator.remove
	//ArrayList may allocate to large of an array! Implement BucketedList instead ! TODO!
	//Also: ArrayList.remove is expensive!! TODO
	private HashMap<Long, CachedObject> _objs = new HashMap<Long,CachedObject>();
	
	private HashSet<CachedObject.CachedSchema> _schemata = new HashSet<CachedObject.CachedSchema>();
	
	private final Session _session;
	private final StateManagerImpl _sm;
	
	public ClientSessionCache(Session session, StateManagerImpl sm) {
		_session = session;
		_sm = sm;
	}
	
	public boolean isSchemaDefined(Class<?> type, Node node) {
//		for (CachedSchema cs: _schemata) {
//			if (cs.getSchema().getSchemaClass() == type &&
//					cs.getNode() == node) {
//				return true;
//			}
//		}
//		for (CachedSchema cs: _dirtySchemata) {
//			if (cs.getSchema().getSchemaClass() == type &&
//					cs.getNode() == node) {
//				return true;
//			}
//		}
//		for (CachedSchema cs: _newSchemata) {
//			if (cs.getSchema().getSchemaClass() == type &&
//					cs.getNode() == node) {
//				return true;
//			}
//		}
//		return false;
		return (findSchemaInCache(type, node) != null);
	}


	public void rollback() {
		//refresh cleans, may have changed in DB
		//Maybe set them all to hollow instead? //TODO
		_objs.clear();
		_schemata.clear();
	}


	public void markPersistent(PersistenceCapableImpl pc, long oid, Node node) {
		CachedObject co;
		co = findCO(pc);
		if (co != null) {
			if (co.isDeleted()) {
				throw new UnsupportedOperationException("Make it persistent again");
				//TODO implement
			}
			//ignore this
			return;
		}
		co = new CachedObject(ObjectState.PERSISTENT_NEW);
		co.obj = pc;
		co.oid = oid;
		co.node = node;
		_objs.put(oid, co);
	}
	
	public CachedObject findCO(PersistenceCapableImpl pc) {
		//TODO implement map<PC, CO>
		for (CachedObject co: _objs.values()) {
			if (co.obj == pc) return co;
		}
		return null;
	}

	public void addHollow(PersistenceCapableImpl obj, Node node) {
		CachedObject co = new CachedObject(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL);
		co.obj = obj;
		co.oid = (Long)obj.jdoGetObjectId();
		co.node = node;
		_objs.put(co.oid, co);
	}

	public void addPC(PersistenceCapableImpl obj, Node node) {
		CachedObject co = new CachedObject(ObjectState.PERSISTENT_CLEAN);
		co.obj = obj;
		co.oid = (Long)obj.jdoGetObjectId();
		co.node = node;
		_objs.put(co.oid, co);
	}

	@Override
	public CachedObject findCoByOID(long oid) {
		return _objs.get(oid);
//		for (CachedObject co: _objs) {
//			if (co.oid == oid) return co;
//		}
//		return null;
	}

	
	public ZooClassDef findSchemaInCache(Class<?> cls, Node node) {
		for (CachedObject.CachedSchema co: _schemata) {
			if (co.getSchema().getSchemaClass().equals(cls) &&
					co.getNode() == node) return co.getSchema();
		}
		return null;
	}

	/**
	 * 
	 * @param node
	 * @return List of objects for that node, EXCLUDING clean objects.
	 */
	public List<CachedObject> getObjectsForCommit(Node node) {
		System.err.println("FIXME ClientNodeCache.getObjectsForCommit()");
		// TODO This currently returns all objects
		ArrayList<CachedObject> objs = new ArrayList<CachedObject>();
		//addAllForNodeCO(_clean, objs, node);  //TODO do not commit _clean objects!
		for (CachedObject co: _objs.values()) {
			if (co.isDirty() && co.getNode() == node) {
				objs.add(co);
			}
		}
		return objs;
	}

	/**
	 * Clean out the cache after commit.
	 * TODO keep hollow objects? E.g. references to correct, e.t.c!
	 */
	public void postCommit() {
		System.err.println("FIXME ClientNodeCache.commit()");
		//TODO later: empty cache (?)
		Iterator<CachedObject> iter = _objs.values().iterator();
		for (; iter.hasNext(); ) {
			CachedObject co = iter.next();
			if (co.isDeleted()) {
				iter.remove();
				continue;
			}
			co.markHollow();
			co.markClean();  //TODO remove if cache is flushed
		}
		Iterator<CachedSchema> iterS = _schemata.iterator();
		for (; iterS.hasNext(); ) {
			CachedSchema cs = iterS.next();
			if (cs.isDeleted()) {
				iterS.remove();
				continue;
			}
			//TODO keep in cache???
			cs.markHollow();
			cs.markClean();  //TODO remove if cache is flushed
		}
	}

	/**
	 * 
	 * @param node
	 * @return List of all cached schema objects for that node (clean, new, deleted, dirty).
	 */
	public List<CachedObject.CachedSchema> getSchemata(Node node) {
		System.err.println("FIXME ClientNodeCache.getSchemataForCommit()");
		ArrayList<CachedObject.CachedSchema> objs = new ArrayList<CachedObject.CachedSchema>();
		for (CachedObject.CachedSchema c: _schemata) {
			if (c.getNode() == node) {
				objs.add(c);
			}
		}
		return objs;
	}
	
	public void addSchema(ZooClassDef clsDef, boolean isLoaded, Node node) {
		ObjectState state = isLoaded ? ObjectState.PERSISTENT_CLEAN : ObjectState.PERSISTENT_NEW;
		CachedObject.CachedSchema cs = new CachedObject.CachedSchema(clsDef, state, node);
		_schemata.add(cs);
	}

	public Object findObjectById(Object id) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	public void deletePersistent(CachedObject co) {
		co.markDeleted();
		//co will be deleted in postCommit();
//		_objs.remove(co);
	}

	public StateManager getStateManager() {
		return _sm;
	}

	public Iterable<CachedObject> getAllObjects() {
		return Collections.unmodifiableCollection(_objs.values());
	}

	/**
	 * TODO Fix this. Schemata should be kept in a separate cache
	 * for each node!
	 * @param def
	 * @param node
	 * @return 
	 */
	public CachedSchema findCachedSchema(Class<?> cls, Node node) {
		for (CachedObject.CachedSchema c: _schemata) {
			if (c.getSchema().getSchemaClass() == cls && c.getNode() == node) {
				return c;
			}
		}
		return null;
	}
}
