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
package org.zoodb.jdo;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.jdo.Extent;
import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.jdo.listener.InstanceLifecycleListener;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.util.DBLogger;
import org.zoodb.jdo.internal.util.TransientField;
import org.zoodb.jdo.internal.util.Util;

/**
 * @author Tilmann Zaeschke
 */
public class PersistenceManagerImpl implements PersistenceManager {

    /**
     * <code>OBJECT_ID_CLASS</code> is the class for all ObjectId instances.
     */
    public static final Class<Long> OBJECT_ID_CLASS = Long.class;
    
    //The final would possibly avoid garbage collection
    private volatile TransactionImpl transaction = null;
    private final PersistenceManagerFactoryImpl factory;
    
    private volatile boolean isClosed = true;
    
    private boolean ignoreCache;
    
    private static final AtomicReference<PersistenceManagerImpl> 
    	defaultSession = new AtomicReference<PersistenceManagerImpl>(null);
    
    private Session nativeConnection;
    
    private final FetchPlan fetchplan = new FetchPlanImpl();
    
    /**
     * @param props
     * @throws JDOUserException for other errors.
     */
    PersistenceManagerImpl(PersistenceManagerFactoryImpl factory, String password) {
        this.factory = factory;
    	nativeConnection = new Session(this, factory.getConnectionURL());
        transaction = new TransactionImpl(this, 
        		factory.getRetainValues(),
        		factory.getOptimistic(),
        		nativeConnection);
		DBLogger.debugPrintln(2, "FIXME: PersistenceManagerImpl()");
        isClosed = false;
        
        ignoreCache = factory.getIgnoreCache(); 
        //FIXME
//        String dbName = getConnectionURL();
//        String userName = getConnectionUserName();
//        //Invalid Password
//        throw new LoginFailedException(
//                    "Invalid Password for database: " + dbName +
//                    " (user=\"" + userName + "\")");
//        //Null password and userName != os-userName
//        throw new LoginFailedException(
//                    "You need to logon as this user to your OS." +
//                    " (user=\"" + userName + "\")");
//        //Invalid User Name
//        throw new LoginFailedException(
//                    "User not registered in database: " + dbName +
//                    " (user=\"" + userName + "\")");
//        throw new JDOUserException("Error while accessing database: " + 
//                dbName + " (user=\"" + userName + "\")" + "SE=\"" + 
//                getSessionName() + "\";", e);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#close()
     */
    @Override
    public void close() {
        if (isClosed) {
            throw new JDOUserException(
                    "PersistenceManager has already been closed.");
        }
        if (transaction.isActive()) {
            //_transaction.rollback();
        	//JDO 2.2 12.6 , are we in a non-managed environment?
        	throw new JDOUserException("Transaction is still active!");
        }
        TransientField.deregisterTx(transaction);
        defaultSession.compareAndSet(this, null);
        nativeConnection.close();
        transaction = null;
        isClosed = true;
        factory.deRegister(this);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#currentTransaction()
     */
    @Override
    public Transaction currentTransaction() {
    	checkOpenIgnoreTx();
        return transaction;
    }
    
    private void checkOpen() {
		checkOpenIgnoreTx();
		//TODO we ignore this for now. Do we need it at all?
		//- polepos-JDO does not use tx for all queries (not even for db4o)
		//- The spec does not seem to say anything
		//- Would it be valid to say that this should only be check in methods on 
		//  currentTransaction()?
//		if (!transaction.isActive()) {
//			throw new JDOFatalUserException("Transaction is closed, missing begin()?");
//		}
	}

    private void checkOpenIgnoreTx() {
		if (isClosed) {
			throw new JDOFatalUserException("PersistenceManager is closed.");
		}
	}

	/**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getExtent(Class, boolean)
     */
    @Override
    public <T> Extent<T> getExtent(Class<T> persistenceCapableClass, boolean subclasses) {
        checkOpen();
        return new ExtentImpl<T>(persistenceCapableClass, subclasses, this, ignoreCache);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#isClosed()
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * @see javax.jdo.PersistenceManager#makePersistent(Object)
     */
    @Override
    public <T> T makePersistent(T pc) {
    	checkOpen();
    	checkPersistence(pc);
    	nativeConnection.makePersistent((ZooPCImpl) pc);
    	return pc;
    }

    private void checkPersistence(Object pc) {
    	if (!(pc instanceof ZooPCImpl)) {
    		throw new JDOUserException("The object is not persistence capable: " +
    				pc.getClass().getName(), pc);
    	}
	}

	/**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#makeTransient(Object)
     */
    @Override
    public void makeTransient(Object pc) {
        checkOpen();
        checkPersistence(pc);
        nativeConnection.makeTransient((ZooPCImpl) pc);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #deletePersistent(java.lang.Object)
     */
    @Override
    public void deletePersistent(Object pc) {
        checkOpen();
        nativeConnection.deletePersistent(pc);
    }

    /**
     * Refreshes and places a ReadLock on the object.
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #refresh(java.lang.Object)
     */
    @Override
    public void refresh(Object pc) {
        checkOpen();
        nativeConnection.refreshObject(pc);
    }

    /**
     * Refreshes and places a ReadLock on the objects.
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #refreshAll(Object...)
     */
    @Override
    public void refreshAll(Object ... pcs) {
        checkOpen();
//        _transaction.refreshObjects(pcs, _transaction.database(), 
//                Constants.RLOCK);
        throw new UnsupportedOperationException();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#evictAll()
     */
    @Override
    public void evictAll() {
        checkOpen();
        nativeConnection.evictAll();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #evictAll(java.util.Collection)
     */
    @SuppressWarnings("rawtypes")
	@Override
    public void evictAll(Collection pcs) {
        checkOpen();
        if (pcs.size() == 0) {
            return;
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #evictAll(Object...)
     */
    @Override
    public void evictAll(Object ... pcs) {
        checkOpen();
        if (pcs.length == 0) {
            return;
        }
        nativeConnection.evictAll(pcs);
    }

    @Override
    public Object getObjectById(Object oid, boolean validate) {
        checkOpen();
        if (oid == null) {
            return null;
        }
        if (validate == false) {
//            throw new UnsupportedOperationException(
//                    "Operation is not supported for validate=false");
        	//according to JDO 2.2 we can choose to fail immediately if the object is not in the 
        	//datastore.
        	//TODO However if it is not in the cache, we need to return a HOLLOW object.
        	//TODO System.out.println("STUB getObjectById(..., false)");
        	return getObjectById(oid);
        } else {
        	return getObjectById(oid);
        }
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getObjectId(java.lang.Object)
     */
    @Override
    public Object getObjectId(Object pc) {
        checkOpen();
        if (pc == null) {
            return null;
        }
        if (! (pc instanceof ZooPCImpl)) {
            return null;
        }
        //use call to JDO API to get 'null' if appropriate
        long oid = ((ZooPCImpl)pc).jdoZooGetOid();
        return ((Long)Session.OID_NOT_ASSIGNED).equals(oid) ? null : oid;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getObjectIdClass(Class)
     */
    @SuppressWarnings("rawtypes")
	@Override
    public Class<?> getObjectIdClass(Class cls) {
        checkOpen();
        if (cls == null) {
            return null;
        }
        return OBJECT_ID_CLASS;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getPersistenceManagerFactory()
     */
    @Override
    public PersistenceManagerFactory getPersistenceManagerFactory() {
        return factory;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #newInstance(Class)
     */
    @Override
    public <T> T newInstance(Class<T> pcClass) {
        checkOpen();
        throw new UnsupportedOperationException();
//        if (!Jod.class.isAssignableFrom(pcClass)) {
//            throw new JDOUserException("Class is not persisten capable: " +
//                    pcClass.getName());
//        }
//        try {
//            return pcClass.newInstance();
//        } catch (InstantiationException e) {
//            throw new RuntimeException(e);
//        } catch (IllegalAccessException e) {
//            throw new RuntimeException(e);
//        }
    }

    /**
     * @see javax.jdo.PersistenceManager#getObjectsById(Collection)
     */
    @SuppressWarnings("rawtypes")
	@Override
    public Collection getObjectsById(Collection oids) {
        checkOpen();
        throw new UnsupportedOperationException();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getObjectsById(Collection)
     */
	@Override
    public Object[] getObjectsById(Object... oids) {
        checkOpen();
        return getObjectsById(Arrays.asList(oids)).toArray();
    }

	@SuppressWarnings("rawtypes")
	@Override
	public void addInstanceLifecycleListener(InstanceLifecycleListener arg0,
			Class... arg1) {
        checkOpen();
        nativeConnection.addInstanceLifecycleListener(arg0, arg1);
	}

	@Override
	public void checkConsistency() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void deletePersistentAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void deletePersistentAll(Collection arg0) {
        checkOpen();
        // TODO optimize
        for (Object o: arg0) {
            deletePersistent(o);
        }
	}

	@Override
	public <T> T detachCopy(T arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Collection<T> detachCopyAll(Collection<T> arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] detachCopyAll(T... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void evict(Object arg0) {
        checkOpen();
		evictAll(arg0);
	}

	
	@SuppressWarnings("rawtypes")
	@Override
	public void evictAll(boolean arg0, Class arg1) {
        checkOpen();
		nativeConnection.evictAll(arg0, arg1);
	}

	@Override
	public void flush() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getCopyOnAttach() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public JDOConnection getDataStoreConnection() {
		checkOpenIgnoreTx();
		return new JDOConnectionImpl(nativeConnection);
	}

	@Override
	public boolean getDetachAllOnCommit() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/**
	 * @see PersistenceManager#getExtent(Class)
	 */
	@Override
	public <T> Extent<T> getExtent(Class<T> cls) {
	    return getExtent(cls, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public FetchGroup getFetchGroup(Class arg0, String arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public FetchPlan getFetchPlan() {
        checkOpen();
        DBLogger.debugPrint(1, "STUB PersistenceManagerImpl.getFetchPlan()");
        return fetchplan;
	}

	@Override
	public boolean getIgnoreCache() {
        checkOpenIgnoreTx();
        return ignoreCache;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set getManagedObjects() {
        checkOpen();
		HashSet<Object> s = new HashSet<Object>();
		for (Object o: nativeConnection.getCachedObjects()) {
			s.add(o);
		}
		return s;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set getManagedObjects(EnumSet<ObjectState> arg0) {
        checkOpen();
		HashSet<Object> s = new HashSet<Object>();
		for (Object o: getManagedObjects()) {
			if (arg0.contains(JDOHelper.getObjectState(o))) {
				s.add(o);
			}
		}
		return s;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set getManagedObjects(Class... arg0) {
        checkOpen();
		HashSet<Object> s = new HashSet<Object>();
		for (Object o: getManagedObjects()) {
			for (Class<?> c: arg0) {
				if (o.getClass() == c) {
					s.add(o);
					break;
				}
			}
		}
		return s;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set getManagedObjects(EnumSet<ObjectState> arg0, Class... arg1) {
        checkOpen();
		HashSet<Object> s = new HashSet<Object>();
		for (Object o: getManagedObjects(arg0)) {
			for (Class<?> c: arg1) {
				if (o.getClass() == c) {
					s.add(o);
					break;
				}
			}
		}
		return s;
	}

	@Override
	public boolean getMultithreaded() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getObjectById(Object arg0) {
        checkOpen();
        Object o = nativeConnection.getObjectById(arg0);
        if (o == null) {
            throw new JDOObjectNotFoundException("OID=" + Util.oidToString(arg0));
        }
        return o;
	}

	@Override
	public <T> T getObjectById(Class<T> arg0, Object arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] getObjectsById(Object[] arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] getObjectsById(boolean arg0, Object... arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Sequence getSequence(String arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Date getServerDate() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getTransactionalObjectId(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getUserObject() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getUserObject(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeNontransactional(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeNontransactionalAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void makeNontransactionalAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] makePersistentAll(T... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Collection<T> makePersistentAll(Collection<T> arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransactional(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransactionalAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void makeTransactionalAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransient(Object arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransientAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void makeTransientAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransientAll(Object[] arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransientAll(boolean arg0, Object... arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void makeTransientAll(Collection arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newNamedQuery(Class arg0, String arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Object newObjectIdInstance(Class arg0, Object arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Query newQuery() {
        checkOpen();
        return new QueryImpl(this);
	}

	@Override
	public Query newQuery(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Query newQuery(String arg0) {
        checkOpen();
		return new QueryImpl(this, arg0);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Class arg0) {
        checkOpen();
		return new QueryImpl(this, arg0, "");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Extent arg0) {
        checkOpen();
        return new QueryImpl(this, arg0, "");
	}

	@Override
	public Query newQuery(String arg0, Object arg1) {
        checkOpen();
		if (arg0.equals(Query.JDOQL)) {
			return newQuery(arg1);
		}
		throw new UnsupportedOperationException("Query type not supported: " + arg0);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Class arg0, Collection arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Class arg0, String arg1) {
        checkOpen();
        return new QueryImpl(this, arg0, arg1);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Extent arg0, String filter) {
        checkOpen();
        return new QueryImpl(this, arg0, filter);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Class arg0, Collection arg1, String arg2) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object putUserObject(Object arg0, Object arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void refreshAll() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void refreshAll(Collection arg0) {
        checkOpen();
		nativeConnection.refreshAll(arg0);
	}

	@Override
	public void refreshAll(JDOException arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeInstanceLifecycleListener(InstanceLifecycleListener arg0) {
        checkOpen();
		nativeConnection.removeInstanceLifecycleListener(arg0);
	}

	@Override
	public Object removeUserObject(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieve(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieve(Object arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void retrieveAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieveAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void retrieveAll(Collection arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieveAll(Object[] arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieveAll(boolean arg0, Object... arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCopyOnAttach(boolean arg0) {
		checkOpenIgnoreTx();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDetachAllOnCommit(boolean arg0) {
		checkOpenIgnoreTx();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setIgnoreCache(boolean arg0) {
        checkOpenIgnoreTx();
		ignoreCache = arg0;
	}

	@Override
	public void setMultithreaded(boolean arg0) {
		checkOpenIgnoreTx();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setUserObject(Object arg0) {
		checkOpenIgnoreTx();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Collection getObjectsById(Collection arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
	
	/**
	 * INTERNAL!
	 */
	public Session getSession() {
		return nativeConnection;
	}

	@Override
	public Integer getDatastoreReadTimeoutMillis() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public Integer getDatastoreWriteTimeoutMillis() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public void setDatastoreReadTimeoutMillis(Integer arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public void setDatastoreWriteTimeoutMillis(Integer arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}
}	
