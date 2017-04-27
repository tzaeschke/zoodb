/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.jdo.Extent;
import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOOptimisticVerificationException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.jdo.listener.InstanceLifecycleListener;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.Session;
import org.zoodb.internal.SessionConfig;
import org.zoodb.internal.SessionParentCallback;
import org.zoodb.internal.ZooHandleImpl;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.DBTracer;
import org.zoodb.internal.util.ObjectIdentitySet;
import org.zoodb.internal.util.TransientField;
import org.zoodb.internal.util.Util;
import org.zoodb.schema.ZooHandle;

/**
 * @author Tilmann Zaeschke
 */
public class PersistenceManagerImpl implements PersistenceManager, SessionParentCallback {

    /**
     * <code>OBJECT_ID_CLASS</code> is the class for all ObjectId instances.
     */
    public static final Class<Long> OBJECT_ID_CLASS = Long.class;
    
    //The final would possibly avoid garbage collection
    private volatile TransactionImpl transaction = null;
    private final PersistenceManagerFactoryImpl factory;

    //TODO remove, use cfg instead.
    private boolean ignoreCache;
    
    private static final AtomicReference<PersistenceManagerImpl> 
    	defaultSession = new AtomicReference<PersistenceManagerImpl>(null);
    
    private final Session nativeConnection;
    private final SessionConfig cfg = new SessionConfig();
    
    private final FetchPlan fetchplan = new FetchPlanImpl();
    
    /**
     * @param props
     * @throws JDOUserException for other errors.
     */
    PersistenceManagerImpl(PersistenceManagerFactoryImpl factory, String password) {
        this.factory = factory;
        cfg.setAutoCreateSchema(factory.getAutoCreateSchema());
        cfg.setEvictPrimitives(factory.getEvictPrimitives());
        cfg.setFailOnCloseQueries(factory.getFailOnClosedQueries());
        cfg.setDetachAllOnCommit(factory.getDetachAllOnCommit());
        cfg.setNonTransactionalRead(factory.getNontransactionalRead());
    	nativeConnection = new Session(this, factory.getConnectionURL(), cfg);
    	nativeConnection.setMultithreaded(factory.getMultithreaded());
        transaction = new TransactionImpl(this, 
        		factory.getRetainValues(),
        		factory.getOptimistic(),
        		nativeConnection);
		DBLogger.debugPrintln(2, "FIXME: PersistenceManagerImpl()");
        
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
     * @see PersistenceManager#close()
     */
    @Override
    public void close() {
    	DBTracer.logCall(this);
        if (isClosed()) {
            throw new JDOUserException("PersistenceManager has already been closed.");
        }
        if (transaction.isActive()) {
            //_transaction.rollback();
        	//JDO 2.2 12.6 , are we in a non-managed environment?
        	throw new JDOUserException("Transaction is still active!");
        }
        TransientField.deregisterTx(nativeConnection);
        defaultSession.compareAndSet(this, null);
        nativeConnection.close();
        transaction = null;
        factory.deRegister(this);
    }

    /**
     * @see PersistenceManager#currentTransaction()
     */
    @Override
    public Transaction currentTransaction() {
    	DBTracer.logCall(this);
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
		if (isClosed()) {
			throw new JDOFatalUserException("PersistenceManager is closed.");
		}
	}

	/**
     * @see PersistenceManager#getExtent(Class, boolean)
     */
    @Override
    public <T> Extent<T> getExtent(Class<T> persistenceCapableClass, boolean subclasses) {
    	DBTracer.logCall(this, persistenceCapableClass, subclasses);
        checkOpen();
        return new ExtentImpl<T>(persistenceCapableClass, subclasses, this, ignoreCache);
    }

    /**
     * @see PersistenceManager#isClosed()
     */
    @Override
    public boolean isClosed() {
    	DBTracer.logCall(this);
        return nativeConnection.isClosed();
    }

    /**
     * @see javax.jdo.PersistenceManager#makePersistent(Object)
     */
    @Override
    public <T> T makePersistent(T pc) {
    	DBTracer.logCall(this, pc);
    	checkOpen();
    	ZooPC zpc = checkPersistence(pc);
    	nativeConnection.makePersistent(zpc);
    	DBTracer.logCall(this, pc);
    	return pc;
    }

    private ZooPC checkPersistence(Object pc) {
    	if (pc instanceof ZooPC) {
    		return (ZooPC) pc;
    	}
    	if (pc instanceof ZooHandleImpl) {
    		return ((ZooHandleImpl)pc).getGenericObject();
    	}
   		throw new JDOUserException("The object is not persistence capable: " +
   				pc.getClass().getName(), pc);
	}

	/**
     * @see PersistenceManager#makeTransient(Object)
     */
    @Override
    public void makeTransient(Object pc) {
    	DBTracer.logCall(this, pc);
        checkOpen();
        ZooPC zpc = checkPersistence(pc);
        nativeConnection.makeTransient(zpc);
    }

    /**
     * @see PersistenceManager#deletePersistent(java.lang.Object)
     */
    @Override
    public void deletePersistent(Object pc) {
    	DBTracer.logCall(this, pc);
        checkOpen();
        nativeConnection.deletePersistent(pc);
    }

    /**
     * Refreshes and places a ReadLock on the object.
     * @see PersistenceManager#refresh(java.lang.Object)
     */
    @Override
    public void refresh(Object pc) {
    	DBTracer.logCall(this, pc);
        checkOpen();
        nativeConnection.refreshObject(pc);
    }

    /**
     * Refreshes and places a ReadLock on the objects.
     * @see PersistenceManager#refreshAll(Object...)
     */
    @Override
    public void refreshAll(Object ... pcs) {
    	DBTracer.logCall(this, pcs);
        checkOpen();
        for (Object o: pcs) {
        	nativeConnection.refreshObject(o);
        }
    }

    /**
     * @see PersistenceManager#evictAll()
     */
    @Override
    public void evictAll() {
    	DBTracer.logCall(this);
        checkOpen();
        nativeConnection.evictAll();
    }

    /**
     * @see PersistenceManager#evictAll(java.util.Collection)
     */
    @SuppressWarnings("rawtypes")
	@Override
    public void evictAll(Collection pcs) {
    	DBTracer.logCall(this, pcs);
        checkOpen();
        if (pcs.size() == 0) {
            return;
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see PersistenceManager#evictAll(Object...)
     */
    @Override
    public void evictAll(Object ... pcs) {
    	DBTracer.logCall(this, pcs);
        checkOpen();
        if (pcs.length == 0) {
            return;
        }
        nativeConnection.evictAll(pcs);
    }

    @Override
    public Object getObjectById(Object oid, boolean validate) {
    	DBTracer.logCall(this, oid, validate);
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
     * @see PersistenceManager#getObjectId(java.lang.Object)
     */
    @Override
    public Object getObjectId(Object pc) {
    	DBTracer.logCall(this, pc);
        checkOpen();
        if (pc == null) {
            return null;
        }
		//Especially for when returning Handles from failed optimistic commit()
		if (pc instanceof ZooHandle) {
			return ((ZooHandle)pc).getOid();
		}
        if (! (pc instanceof ZooPC)) {
            return null;
        }
        //use call to JDO API to get 'null' if appropriate
        long oid = ((ZooPC)pc).jdoZooGetOid();
        return ((Long)Session.OID_NOT_ASSIGNED).equals(oid) ? null : oid;
    }

    /**
     * @see PersistenceManager#getObjectIdClass(Class)
     */
    @SuppressWarnings("rawtypes")
	@Override
    public Class<?> getObjectIdClass(Class cls) {
    	DBTracer.logCall(this, cls);
        checkOpen();
        if (cls == null) {
            return null;
        }
        return OBJECT_ID_CLASS;
    }

    /**
     * @see PersistenceManager#getPersistenceManagerFactory()
     */
    @Override
    public PersistenceManagerFactory getPersistenceManagerFactory() {
    	DBTracer.logCall(this);
        return factory;
    }

    /**
     * @see PersistenceManager#newInstance(Class)
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
    	DBTracer.logCall(this, oids);
        checkOpen();
        throw new UnsupportedOperationException();
    }

    /**
     * @see PersistenceManager#getObjectsById(Collection)
     */
	@Override
    public Object[] getObjectsById(Object... oids) {
    	DBTracer.logCall(this, oids);
        checkOpen();
        return getObjectsById(Arrays.asList(oids)).toArray();
    }

	@SuppressWarnings("rawtypes")
	@Override
	public void addInstanceLifecycleListener(InstanceLifecycleListener arg0, Class... arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
        nativeConnection.addInstanceLifecycleListener(arg0, arg1);
	}

	@Override
	public void checkConsistency() {
    	DBTracer.logCall(this);
        checkOpen();
		nativeConnection.checkConsistency();
	}

	@Override
	public void deletePersistentAll(Object... arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
        for (Object o: arg0) {
            deletePersistent(o);
        }
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void deletePersistentAll(Collection arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
        for (Object o: arg0) {
            deletePersistent(o);
        }
	}

	@Override
	public <T> T detachCopy(T arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Collection<T> detachCopyAll(Collection<T> arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	@SafeVarargs
	public final <T> T[] detachCopyAll(T... arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void evict(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		evictAll(arg0);
	}

	
	@SuppressWarnings("rawtypes")
	@Override
	public void evictAll(boolean arg0, Class arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		nativeConnection.evictAll(arg0, arg1);
	}

	@Override
	public void flush() {
    	DBTracer.logCall(this);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getCopyOnAttach() {
    	DBTracer.logCall(this);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public JDOConnection getDataStoreConnection() {
    	DBTracer.logCall(this);
		checkOpenIgnoreTx();
		return new JDOConnectionImpl(nativeConnection);
	}

	@Override
	public boolean getDetachAllOnCommit() {
    	DBTracer.logCall(this);
        checkOpen();
		return cfg.getDetachAllOnCommit();
	}

	/**
	 * @see PersistenceManager#getExtent(Class)
	 */
	@Override
	public <T> Extent<T> getExtent(Class<T> cls) {
    	DBTracer.logCall(this, cls);
	    return getExtent(cls, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public FetchGroup getFetchGroup(Class arg0, String arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public FetchPlan getFetchPlan() {
    	DBTracer.logCall(this);
        checkOpen();
        DBLogger.debugPrint(1, "STUB PersistenceManagerImpl.getFetchPlan()");
        return fetchplan;
	}

	@Override
	public boolean getIgnoreCache() {
    	DBTracer.logCall(this);
        checkOpenIgnoreTx();
        return ignoreCache;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set getManagedObjects() {
    	DBTracer.logCall(this);
        checkOpen();
        return nativeConnection.getCachedObjects();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set getManagedObjects(EnumSet<ObjectState> arg0) {
    	DBTracer.logCall(this, arg0);
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
    	DBTracer.logCall(this, (Object[])arg0);
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
    	DBTracer.logCall(this, arg0, arg1);
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
    	DBTracer.logCall(this);
        checkOpen();
        return nativeConnection.getMultithreaded();
	}

	@Override
	public Object getObjectById(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
        Object o = nativeConnection.getObjectById(arg0);
        if (o == null) {
            throw new JDOObjectNotFoundException("OID=" + Util.oidToString(arg0));
        }
        return o;
	}

	@Override
	public <T> T getObjectById(Class<T> arg0, Object arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] getObjectsById(Object[] arg0, boolean arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] getObjectsById(boolean arg0, Object... arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Sequence getSequence(String arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Date getServerDate() {
    	DBTracer.logCall(this);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getTransactionalObjectId(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getUserObject() {
    	DBTracer.logCall(this);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getUserObject(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeNontransactional(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeNontransactionalAll(Object... arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void makeNontransactionalAll(Collection arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	@SafeVarargs
	public final <T> T[] makePersistentAll(T... arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Collection<T> makePersistentAll(Collection<T> arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransactional(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransactionalAll(Object... arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void makeTransactionalAll(Collection arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransient(Object arg0, boolean arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransientAll(Object... arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void makeTransientAll(Collection arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransientAll(Object[] arg0, boolean arg1) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void makeTransientAll(boolean arg0, Object... arg1) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void makeTransientAll(Collection arg0, boolean arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newNamedQuery(Class arg0, String arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Object newObjectIdInstance(Class arg0, Object arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Query newQuery() {
    	DBTracer.logCall(this);
        checkOpen();
        return new QueryImpl(this);
	}

	@Override
	public Query newQuery(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Query newQuery(String arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		return new QueryImpl(this, arg0);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Class arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		return new QueryImpl(this, arg0, "");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Extent arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
        return new QueryImpl(this, arg0, "");
	}

	@Override
	public Query newQuery(String arg0, Object arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		if (arg0.equals(Query.JDOQL)) {
			return newQuery(arg1);
		}
		throw new UnsupportedOperationException("Query type not supported: " + arg0);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Class arg0, Collection arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		Query q = new QueryImpl(this, arg0, "");
		q.setCandidates(arg1);
		return q;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Class arg0, String arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
        return new QueryImpl(this, arg0, arg1);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Extent arg0, String filter) {
    	DBTracer.logCall(this, arg0, filter);
        checkOpen();
        return new QueryImpl(this, arg0, filter);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Query newQuery(Class arg0, Collection arg1, String arg2) {
    	DBTracer.logCall(this, arg0, arg1, arg2);
        checkOpen();
		Query q = new QueryImpl(this, arg0, arg2);
		q.setCandidates(arg1);
		return q;
	}

	@Override
	public Object putUserObject(Object arg0, Object arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void refreshAll() {
    	DBTracer.logCall(this);
        checkOpen();
		nativeConnection.refreshAll();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void refreshAll(Collection arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		nativeConnection.refreshAll(arg0);
	}

	@Override
	public void refreshAll(JDOException arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
        //We can't use HashSet because it may call an object's hashCode() and activateRead().
		ObjectIdentitySet<Object> failed = new ObjectIdentitySet<>();
		for (Throwable t: arg0.getNestedExceptions()) {
			Object f = ((JDOOptimisticVerificationException)t).getFailedObject();
			failed.add(f);
		}
		nativeConnection.refreshAll(failed);
	}

	@Override
	public void removeInstanceLifecycleListener(InstanceLifecycleListener arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		nativeConnection.removeInstanceLifecycleListener(arg0);
	}

	@Override
	public Object removeUserObject(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieve(Object arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieve(Object arg0, boolean arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void retrieveAll(Collection arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieveAll(Object... arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void retrieveAll(Collection arg0, boolean arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieveAll(Object[] arg0, boolean arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void retrieveAll(boolean arg0, Object... arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCopyOnAttach(boolean arg0) {
    	DBTracer.logCall(this, arg0);
		checkOpenIgnoreTx();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDetachAllOnCommit(boolean arg0) {
    	DBTracer.logCall(this, arg0);
		checkOpenIgnoreTx();
		cfg.setDetachAllOnCommit(arg0);
	}

	@Override
	public void setIgnoreCache(boolean arg0) {
    	DBTracer.logCall(this, arg0);
        checkOpenIgnoreTx();
		ignoreCache = arg0;
	}

	@Override
	public void setMultithreaded(boolean arg0) {
    	DBTracer.logCall(this, arg0);
		checkOpenIgnoreTx();
		nativeConnection.setMultithreaded(arg0);
	}

	@Override
	public void setUserObject(Object arg0) {
    	DBTracer.logCall(this, arg0);
		checkOpenIgnoreTx();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Collection getObjectsById(Collection arg0, boolean arg1) {
    	DBTracer.logCall(this, arg0, arg1);
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
	
	/**
	 * INTERNAL!
	 * @return The native session object
	 */
	public Session getSession() {
		return nativeConnection;
	}

	@Override
	public Integer getDatastoreReadTimeoutMillis() {
    	DBTracer.logCall(this);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public Integer getDatastoreWriteTimeoutMillis() {
    	DBTracer.logCall(this);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public void setDatastoreReadTimeoutMillis(Integer arg0) {
    	DBTracer.logCall(this, arg0);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public void setDatastoreWriteTimeoutMillis(Integer arg0) {
    	DBTracer.logCall(this, arg0);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public Map<String, Object> getProperties() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public Set<String> getSupportedProperties() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}
}	
