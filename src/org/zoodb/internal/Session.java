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
package org.zoodb.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.WeakHashMap;

import javax.jdo.JDOOptimisticVerificationException;
import javax.jdo.ObjectState;
import javax.jdo.listener.DeleteCallback;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.jdo.listener.StoreCallback;

import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.client.SchemaManager;
import org.zoodb.internal.client.session.ClientSessionCache;
import org.zoodb.internal.server.OptimisticTransactionResult;
import org.zoodb.internal.server.TxObjInfo;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.IteratorRegistry;
import org.zoodb.internal.util.MergingIterator;
import org.zoodb.internal.util.TransientField;
import org.zoodb.internal.util.Util;
import org.zoodb.schema.ZooSchema;
import org.zoodb.tools.ZooHelper;

/**
 * The main session class.
 * 
 * @author ztilmann
 *
 */
public class Session implements IteratorRegistry {

	public static final long OID_NOT_ASSIGNED = -1;
	public static final long TIMESTAMP_NOT_ASSIGNED = -1;

	public static final Class<?> PERSISTENT_SUPER = ZooPC.class;
	
	/** Primary node. Also included in the _nodes list. */
	private Node primary;
	/** All connected nodes. Includes the primary node. */
	private final ArrayList<Node> nodes = new ArrayList<Node>();
	private final SessionParentCallback parentSession;
	private final ClientSessionCache cache;
	private final SchemaManager schemaManager;
	private boolean isOpen = true;
	private boolean isActive = false;
	private final SessionConfig config;
	
	private long transactionId = -1;
	
	private final WeakHashMap<CloseableIterator<?>, Object> extents = 
	    new WeakHashMap<CloseableIterator<?>, Object>(); 
	
	public Session(String dbPath, SessionConfig config) {
		this(null, dbPath, config);
	}
	
	public Session(SessionParentCallback parentSession, String dbPath, SessionConfig config) {
		dbPath = ZooHelper.getDataStoreManager().getDbPath(dbPath);
		this.parentSession = parentSession;
		this.config = config;
		this.primary = ZooFactory.get().createNode(dbPath, this);
		this.cache = new ClientSessionCache(this);
		this.schemaManager = new SchemaManager(cache, config.getAutoCreateSchema());
		this.nodes.add(primary);
		this.cache.addNode(primary);
		this.primary.connect();
	}
	
	public boolean isActive() {
		return isActive;
	}
	
	public void begin() {
		checkOpen();
        if (isActive) {
            throw DBLogger.newUser("Can't open new transaction inside existing transaction.");
        }
		isActive = true;
		for (Node n: nodes) {
			long txId = n.beginTransaction();
			if (n == primary) {
				transactionId = txId;
			}
		}
	}
	
	/**
	 * Verify optimistic consistency of the current transaction.
	 * 
	 * @throws JDOOptimisticVerificationException containing all failed objects if
	 * any objects fail.
	 */
	public void checkConsistency() {
		processOptimisticVerification(true);
	}

	public void commit(boolean retainValues) {
		checkActive();
		
		//pre-commit: traverse object tree for transitive persistence
		cache.persistReachableObjects();

		//commit phase #1: prepare, check conflicts, get optimistic locks
		//This needs to happen after OGT (we need the OIDs) and before everything else (avoid
		//any writes in case of conflict AND we need the WLOCK before any updates.
		processOptimisticVerification(false);

		try {
			schemaManager.commit();

			commitInternal();
			//commit phase #2: Updated database properly, release locks
			for (Node n: nodes) {
				n.commit();
			}
			cache.postCommit(retainValues, config.getDetachAllOnCommit());
			schemaManager.postCommit();
		} catch (RuntimeException e) {
			if (DBLogger.isUser(e)) {
				//reset sinks
				for (ZooClassDef cs: cache.getSchemata()) {
					cs.getProvidedContext().getDataSink().reset();
					cs.getProvidedContext().getDataDeleteSink().reset();
				}		
				//allow for retry after user exceptions
				for (Node n: nodes) {
					n.revert();
				}
			}
			rollback();
			throw e;
		}

		for (CloseableIterator<?> ext: extents.keySet().toArray(new CloseableIterator[0])) {
			//TODO remove, is this still useful?
			ext.close();
		}
		isActive = false;
	}


	private void getObjectToCommit(ArrayList<TxObjInfo> updates) {
		for (ZooPC pc: cache.getDeletedObjects()) {
			updates.add(new TxObjInfo(pc.jdoZooGetOid(), pc.jdoZooGetTimestamp(), true));
		}
		for (ZooPC pc: cache.getDirtyObjects()) {
			updates.add(new TxObjInfo(pc.jdoZooGetOid(), pc.jdoZooGetTimestamp(), false));
		}
		for (GenericObject pc: cache.getDirtyGenericObjects()) {
			updates.add(new TxObjInfo(pc.getOid(), pc.jdoZooGetTimestamp(), pc.jdoZooIsDeleted()));
		}
		for (ZooClassDef cd: cache.getSchemata()) {
			if (cd.jdoZooIsDeleted() || cd.jdoZooIsNew() || cd.jdoZooIsDirty()) {
				updates.add(new TxObjInfo(cd.jdoZooGetOid(), cd.jdoZooGetTimestamp(), 
						cd.jdoZooIsDeleted()));
			}
		}
	}

	private void processOptimisticVerification(boolean isTrialRun) {
		ArrayList<TxObjInfo> updates = new ArrayList<>();
		getObjectToCommit(updates);
		OptimisticTransactionResult ovrSummary = new OptimisticTransactionResult();
		for (Node n: nodes) {
			if (isTrialRun) {
				//check consistency
				ovrSummary.add( n.checkTxConsistency(updates) );
			} else {
				//proper commit()
				ovrSummary.add( n.beginCommit(updates) );
			}
		}
		
		processOptimisticTransactionResult(ovrSummary);
		
		if (!ovrSummary.getConflicts().isEmpty()) {
			JDOOptimisticVerificationException[] ea = 
					new JDOOptimisticVerificationException[ovrSummary.getConflicts().size()];
			int pos = 0;
			for (Long oid: ovrSummary.getConflicts()) {
				Object failedObj = cache.findCoByOID(oid); 
				if (failedObj == null) {
					//generic object
					failedObj = cache.getGeneric(oid).getOrCreateHandle();
				}
				ea[pos] = new JDOOptimisticVerificationException(Util.oidToString(oid), failedObj);
				pos++;
			}
			if (!isTrialRun) {
				//perform rollback
				rollback();
			}
			throw new JDOOptimisticVerificationException("Optimistic verification failed", ea);
		}
	}
	
	private void processOptimisticTransactionResult(OptimisticTransactionResult otr) {
		if (otr.requiresReset()) {
			isActive = false;
			closeInternal();
			throw DBLogger.newFatalDataStore(
					"Database schema has changed, please reconnect. ", null);
		}
		if (otr.requiresRefresh()) {
			if (schemaManager.hasChanges()) {
				//remote index update & local schema updates (could be index) --> considered bad!
				throw new JDOOptimisticVerificationException("Optimistic verification failed "
						+ "because schema changes occurred in remote concurrent sessions.");
			}

			// refresh schema, this works only for indexes
			schemaManager.refreshSchemaAll();
		}
	}
	
	private void commitInternal() {
		//create new schemata
		Collection<ZooClassDef> schemata = cache.getSchemata();
		
		//First delete
		for (ZooPC co: cache.getDeletedObjects()) {
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
    			if (go.jdoZooIsDeleted() && !go.jdoZooIsNew()) {
    				if (!go.checkPcDeleted()) {
    					go.jdoZooGetContext().getDataDeleteSink().deleteGeneric(go);
    				}
    			}
    		}
		}
		//flush sinks
        for (ZooClassDef cs: schemata) {
            cs.getProvidedContext().getDataDeleteSink().flush();
        }		

        //Then update. This matters for unique indices where deletion must occur before updates.
		for (ZooPC co: cache.getDirtyObjects()) {
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
    			if (!go.jdoZooIsDeleted()) {
    				go.verifyPcNotDirty();
	    		    go.toStream();
	                go.jdoZooGetContext().getDataSink().writeGeneric(go);
    			}
    		}
		}
		
		//flush sinks
        for (ZooClassDef cs: schemata) {
            cs.getProvidedContext().getDataSink().flush();
        }		
	}

	public void rollback() {
		checkActive();
		schemaManager.rollback();
		
		OptimisticTransactionResult otr = new OptimisticTransactionResult();
		for (Node n: nodes) {
			otr.add( n.rollbackTransaction() );
		}
		cache.rollback();
		isActive = false;

		processOptimisticTransactionResult(otr);
	}
	
	public void makePersistent(ZooPC pc) {
		checkActive();
		if (pc.jdoZooIsPersistent()) {
			if (pc.jdoZooGetContext().getSession() != this) {
				throw DBLogger.newUser("The object belongs to a different persistence manager.");
			}
			if (pc.jdoZooIsDeleted()) {
				throw DBLogger.newUser("The object has been deleted!");
			}
			//nothing to do, is already persistent
			return; 
		}
		primary.makePersistent(pc);
	}

	public void makeTransient(ZooPC pc) {
		checkActive();
		if (!pc.jdoZooIsPersistent()) {
			//already transient
			return;
		}
		if (pc.jdoZooGetContext().getSession() != this) {
			throw DBLogger.newUser("The object belongs to a different persistence manager.");
		}
		if (pc.jdoZooIsDirty()) {
			throw DBLogger.newUser(
					"Dirty objects can not be made transient: " + Util.getOidAsString(pc));
		}
		//remove from cache
		cache.makeTransient((ZooPC) pc);
	}

	public static void assertOid(long oid) {
		if (oid == OID_NOT_ASSIGNED) {
			throw DBLogger.newUser("Invalid OID: " + oid);
		}
	}

	public MergingIterator<ZooPC> loadAllInstances(Class<?> cls, 
			boolean subClasses, 
            boolean loadFromCache) {
		checkActive();
		MergingIterator<ZooPC> iter = 
			new MergingIterator<ZooPC>(this);
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
			MergingIterator<ZooPC> iter, boolean loadFromCache) {
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
		checkActive();
		GenericObject gob = cache.getGeneric(oid);
		if (gob != null) {
			return gob.getOrCreateHandle();
		}
		
		ZooPC co = cache.findCoByOID(oid);
        if (co != null) {
        	if (co.jdoZooIsNew() || co.jdoZooIsDirty()) {
        		//TODO  the problem here is the initialisation of the GO, which would require
        		//a way to serialize PCs into memory and deserialize them into an GO
        		throw new UnsupportedOperationException("Handles on new or dirty Java PC objects " +
        				"are not allowed. Please call commit() first or create handles with " +
        				"ZooClass.newInstance() instead. OID: " + Util.getOidAsString(co));
        	}
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
        } catch (RuntimeException e) {
        	if (!DBLogger.isObjectNotFoundException(e)) {
        		throw e;
        	}
        	//ignore, return null
        }
        return null;
	}

	public ZooHandleImpl getHandle(Object pc) {
		checkActive();
		ZooPC pci = checkObject(pc);
		long oid = pci.jdoZooGetOid();
		GenericObject gob = cache.getGeneric(oid);
		if (gob != null) {
			return gob.getOrCreateHandle();
		}
		
		if (pci.jdoZooIsNew() || pci.jdoZooIsDirty()) {
			//TODO  the problem here is the initialisation of the GO, which would require
			//a way to serialize PCs into memory and deserialize them into an GO
			throw new UnsupportedOperationException("Handles on new or dirty Java PC objects " +
					"are not allowed. Please call commit() first or create handles with " +
					"ZooClass.newInstance() instead. OID: " + Util.getOidAsString(pci));
		}
		ZooClassDef schema = pci.jdoZooGetClassDef();
		GenericObject go = pci.jdoZooGetNode().readGenericObject(schema, oid);
		return go.getOrCreateHandle();
	}

	/**
	 * Refresh an Object. If the object has been deleted locally, it will
	 * get the state of the object on disk. 
	 * @param pc
	 */
	public void refreshObject(Object pc) {
        ZooPC co = checkObjectForRefresh(pc);
        if (co.jdoZooIsPersistent()) {
        	co.jdoZooGetNode().refreshObject(co);
        }
 	}
	

	public void refreshAll() {
		checkActive();
		ArrayList<ZooPC> objs = new ArrayList<>();
		for ( ZooPC pc: cache.getAllObjects() ) {
	        ZooPC co = checkObjectForRefresh(pc);
	        if (co.jdoZooIsPersistent()) {
	        	objs.add(co);
	        }
		}
		//We use a separate loop here to avoid concurrent-mod exceptions in cases where a 
		//remotely deleted object has to be removed from the local cache.
		for (ZooPC pc: objs) {
			try {
				refreshObject(pc);
			} catch (RuntimeException t) {
				if (DBLogger.OBJ_NOT_FOUND_EXCEPTION.isAssignableFrom(t.getClass())) {
					//okay, ignore, this happens if an object was delete remotely
					continue;
				}
				throw t;
			}
		}
	}


	public void refreshAll(Collection<?> arg0) {
		checkActive();
		for ( Object obj: arg0 ) {
			refreshObject(obj);
		}
	}


	/**
	 * Check for base class, persistence state and PM affiliation. 
	 * @param pc
	 * @return CachedObject
	 */
	private ZooPC checkObject(Object pc) {
        return checkObject(pc, false);
	}

	private ZooPC checkObject(Object pc, boolean ignoreForRefresh) {
        if (!(pc instanceof ZooPC)) {
        	throw DBLogger.newUser("The object is not persistent capable: " + pc.getClass());
        }
        
        ZooPC pci = (ZooPC) pc;
        if (!ignoreForRefresh && !pci.jdoZooIsPersistent()) {
        	throw DBLogger.newUser("The object has not been made persistent yet.");
        }
        if (!ignoreForRefresh && pci.jdoZooIsDeleted()) {
        	throw DBLogger.newUser("The object has alerady been deleted.");
        }

        if (pci.jdoZooGetContext().getSession() != this) {
        	throw DBLogger.newUser("The object belongs to a different PersistenceManager.");
        }
        return pci;
	}


	/**
	 * For refresh, we can ignore things like deletion or transience.
	 * @param pc
	 * @return
	 */
	private ZooPC checkObjectForRefresh(Object pc) {
        if (!(pc instanceof ZooPC)) {
        	throw DBLogger.newUser("The object is not persistent capable: " + pc.getClass());
        }
        
        ZooPC pci = (ZooPC) pc;
        if (!pci.jdoZooIsPersistent()) {
        	return pci;
        }

        if (pci.jdoZooGetContext().getSession() != this) {
        	throw DBLogger.newUser("The object belongs to a different PersistenceManager.");
        }
        return pci;
	}


	public Object getObjectById(Object arg0) {
		checkActive();
        long oid = (Long) arg0;
        ZooPC co = cache.findCoByOID(oid);
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
		checkActive();
		Object[] res = new Object[arg0.size()];
		int i = 0;
		for ( Object obj: arg0 ) {
			res[i] = getObjectById(obj);
			i++;
		}
		return res;
	}

	/**
	 * @param oid
	 * @return Whether the object exists
	 */
	public boolean isOidUsed(long oid) {
		checkActive();
		//TODO we could also just compare it with max-value in the OID manager...
        ZooPC co = cache.findCoByOID(oid);
        if (co != null) {
        	return true;
        }
        GenericObject go = cache.getGeneric(oid);
        if (go != null) {
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
		checkActive();
		ZooPC co = checkObject(pc);
		co.jdoZooMarkDeleted();
	}


	public SchemaManager getSchemaManager() {
		checkOpen();
		return schemaManager;
	}


	public void close() {
		if (!isOpen) {
			throw DBLogger.newUser("This session is closed.");
		}
		for (Node n: nodes) {
			n.closeConnection();
		}
		cache.close();
		TransientField.deregisterPm(this);
		isOpen = false;
	}

	private void closeInternal() {
		if (parentSession != null) {
			parentSession.close();
		} else {
			close();
		}
	}
	
    public Object getExternalSession() {
		checkOpen();
        return parentSession;
    }


    public SessionConfig getConfig() {
		checkOpen();
        return config;
    }


    public void evictAll() {
		checkActive();
        cache.evictAll();
    }


    public void evictAll(Object[] pcs) {
		checkActive();
    	for (Object obj: pcs) {
    		ZooPC pc = (ZooPC) obj;
    		if (!pc.jdoZooIsDirty()) {
    			pc.jdoZooEvict();
    		}
    	}
    }


    public void evictAll(boolean subClasses, Class<?> cls) {
		checkActive();
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


    public Collection<ZooPC> getCachedObjects() {
		checkActive();
        HashSet<ZooPC> ret = new HashSet<ZooPC>();
        for (ZooPC o: cache.getAllObjects()) {
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
		checkOpen();
		if (classes == null) {
			classes = new Class[]{null};
		}
		for (Class<?> cls: classes) {
			if (cls == null) {
				cls = ZooPC.class;
			}
			ZooClassDef def = cache.getSchema(cls, primary);
			if (def == null) {
				throw DBLogger.newUser("Cannot define listener for unknown class: " + cls);
			}
			def.getProvidedContext().addLifecycleListener(listener);
		}
	}


	public void removeInstanceLifecycleListener(InstanceLifecycleListener listener) {
		checkActive();
		for (ZooClassDef def: cache.getSchemata()) {
			def.getProvidedContext().removeLifecycleListener(listener);
		}
	}

	private void checkActive() {
    	if (!isActive) {
    		throw DBLogger.newUser("Transaction is not active. Missing 'begin()'?");
    	}
    	checkOpen();
	}
	
	private void checkOpen() {
		if (!isOpen) {
			throw DBLogger.newUser("This session is closed.");
		}
	}

	public boolean isClosed() {
		return !isOpen;
	}

	public static long getObjectId(Object o) {
		if (o instanceof ZooPC) {
			DBLogger.newUser("The object is not persistence capable: " + o.getClass());
		}
		ZooPC zpc = (ZooPC) o;
		return zpc.jdoZooGetOid();
	}
	
	public static Session getSession(Object o) {
		if (o instanceof ZooPC) {
			DBLogger.newUser("The object is not persistence capable: " + o.getClass());
		}
		ZooPC zpc = (ZooPC) o;
		if (zpc.jdoZooGetContext() == null) {
			return null;
		}
		return zpc.jdoZooGetContext().getSession();
	}
	
	/**
	 * Get access to schema management.
	 * @return Schema management API
	 */
	public ZooSchema schema() {
		return new ZooSchemaImpl(this, schemaManager);
	}
	
	public long getTransactionId() {
		return transactionId;
	}
}
