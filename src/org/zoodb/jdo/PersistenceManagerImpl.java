package org.zoodb.jdo	;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.jdo.Extent;
import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.jdo.spi.PersistenceCapable;

import org.zoodb.jdo.custom.JDOAuthenticationException;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.TransientField;

/**
 * @author Tilmann Zaeschke
 */
public class PersistenceManagerImpl implements PersistenceManager {

    /**
     * <code>OBJECT_ID_CLASS</code> is the class for all ObjectId instances.
     */
    public static final Class<Long> OBJECT_ID_CLASS = Long.class;
    
    //The final would possibly avoid garbage collection
    private volatile TransactionImpl _transaction = null;
    private final PersistenceManagerFactory _factory;
    
    private volatile boolean _isClosed = true;
    
    private static final AtomicReference<PersistenceManagerImpl> 
    	_defaultSession = new AtomicReference<PersistenceManagerImpl>(null);
    
    private Session _nativeConnection;
    
    /**
     * @param props
     * @throws JDOAuthenticationException for authentication errors.
     * @throws JDOUserException for other errors.
     */
    PersistenceManagerImpl(PersistenceManagerFactory factory, String password) {
    	_nativeConnection = new Session(this, factory.getConnectionURL());
        _factory = factory;
        Properties dbProps = createProperties(_factory, password);
        _transaction = new TransactionImpl(dbProps, this, 
        		_factory.getRetainValues(),
        		_factory.getOptimistic(),
//        		_factory.getAutoJoinThreads()
        		true, _nativeConnection);
        System.err.println("FIXME: PersistenceManagerImpl()");
        //FIXME _transaction.loidCapacity(_factory.getLoidAllocation());
        _isClosed = false;
        
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

    private Properties createProperties(PersistenceManagerFactory fact,
            String password) {
        Properties props = new Properties();

        return props;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#close()
     */
    public void close() {
        if (_isClosed) {
            throw new JDOUserException(
                    "PersistenceManager has already been closed.");
        }
        if (_transaction.isActive()) {
            //_transaction.rollback();
        	//JDO 2.2 12.6 , are we in a non-managed environment?
        	throw new JDOUserException("Transaction is still active!");
        }
        TransientField.deregisterTx(_transaction);
        _defaultSession.compareAndSet(this, null);
        _nativeConnection.close();
        _transaction = null;
        _isClosed = true;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#currentTransaction()
     */
    public Transaction currentTransaction() {
    	checkOpen();
        return _transaction;
    }
    
    private void checkOpen() {
		if (_isClosed) {
			throw new JDOFatalUserException("PersistenceManager is closed.");
		}
	}

	/**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getExtent(Class, boolean)
     */
    public <T> Extent<T> getExtent(Class<T> persistenceCapableClass, 
            boolean subclasses) {
        checkOpen();
        return new ExtentImpl<T>(persistenceCapableClass, subclasses, this, 
        		PersistenceCapableImpl.class);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#isClosed()
     */
    public boolean isClosed() {
        return _isClosed;
    }

    /**
     * @see javax.jdo.PersistenceManager#makePersistent(Object)
     */
	public <T> T makePersistent(T pc) {
        checkOpen();
        checkPersistence(pc);
       _nativeConnection.makePersistent((PersistenceCapableImpl) pc);
       return pc;
    }

    private void checkPersistence(Object pc) {
    	if (!(pc instanceof PersistenceCapable)) {
    		throw new JDOUserException("The object is not persistence capable: " +
    				pc.getClass().getName(), 
    				pc);
    	}
	}

	/**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#makeTransient(Object)
     */
    public void makeTransient(Object pc) {
        checkOpen();
        _nativeConnection.makeTransient(pc);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #deletePersistent(java.lang.Object)
     */
    public void deletePersistent(Object pc) {
        checkOpen();
        _nativeConnection.deletePersistent(pc);
    }

    /**
     * Refreshes and places a ReadLock on the object.
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #refresh(java.lang.Object)
     */
    public void refresh(Object pc) {
        checkOpen();
//        _transaction.refreshObjects(new Object[]{pc}, 
//                _transaction.database(), Constants.RLOCK);
        throw new UnsupportedOperationException();
    }

    /**
     * Refreshes and places a ReadLock on the objects.
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #refreshAll(Object...)
     */
    public void refreshAll(Object ... pcs) {
        checkOpen();
//        _transaction.refreshObjects(pcs, _transaction.database(), 
//                Constants.RLOCK);
        throw new UnsupportedOperationException();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager#evictAll()
     */
    public void evictAll() {
        checkOpen();
        throw new UnsupportedOperationException(
                "evictAll() is not supported in this release.");
        //_transaction.zapObjectsExcept(new Object[]{}, true);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #evictAll(java.util.Collection)
     */
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
    public void evictAll(Object ... pcs) {
        checkOpen();
        if (pcs.length == 0) {
            return;
        }
        throw new UnsupportedOperationException();
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
            return _nativeConnection.getObjectsById(oid)[0];
        } else {
            return _nativeConnection.getObjectsById(oid)[0];
        }
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getObjectId(java.lang.Object)
     */
    public Object getObjectId(Object pc) {
        checkOpen();
        if (pc == null) {
            return null;
        }
        if (! (pc instanceof PersistenceCapable)) {
            return null;
        }
        return ((PersistenceCapableImpl)pc).jdoGetObjectId();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getObjectIdClass(Class)
     */
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
    public PersistenceManagerFactory getPersistenceManagerFactory() {
        return _factory;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #newInstance(Class)
     */
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
    @Override
    public Collection getObjectsById(Collection oids) {
        checkOpen();
        throw new UnsupportedOperationException();
//        if (oids.isEmpty()) {
//            return null;
//        }
//
//        long loid = -1;
//        try {
//            ArrayList<Object> objs = new ArrayList<Object>();
//            for (Object oid: oids) {
//                if (oid.getClass() != OBJECT_ID_CLASS) {
//                    throw new IllegalArgumentException(
//                            "Unexpected ObjectId class: " + oid.getClass());
//                }
//                loid = ((Long)oid).longValue();
//                objs.add(_transaction.handleToObject(
//                        _transaction.newHandle(loid)));
//            }
//            // TODO implement group read.
//            return objs;
//        } catch (VException e) {
//            //No such object
//            if (e.getErrno() == 5006) {
//                throw new JDOObjectNotFoundException("Object not found: " +
//                        DatabaseTools.idToString(loid));
//            }
//            throw new JDOUserException(e.getMessage(), e);
//        }
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManager
     * #getObjectsById(Collection)
     */
    public Object[] getObjectsById(Object[] oids) {
        checkOpen();
        return getObjectsById(Arrays.asList(oids)).toArray();
    }

	public void addInstanceLifecycleListener(InstanceLifecycleListener arg0,
			Class... arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void checkConsistency() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void deletePersistentAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void deletePersistentAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public <T> T detachCopy(T arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public <T> Collection<T> detachCopyAll(Collection<T> arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public <T> T[] detachCopyAll(T... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void evict(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void evictAll(boolean arg0, Class arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void flush() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getCopyOnAttach() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public JDOConnection getDataStoreConnection() {
        checkOpen();
		//TODO set _nativeConnection to 'null' ??????
		return new JDOConnectionImpl(_nativeConnection);
	}

	public boolean getDetachAllOnCommit() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	/**
	 * @see PersistenceManager#getExtent(Class)
	 */
	public <T> Extent<T> getExtent(Class<T> cls) {
        checkOpen();
		return new ExtentImpl(cls, true, this, PersistenceCapableImpl.class);
	}

	public FetchGroup getFetchGroup(Class arg0, String arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public FetchPlan getFetchPlan() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getIgnoreCache() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Set getManagedObjects() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Set getManagedObjects(EnumSet<ObjectState> arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Set getManagedObjects(Class... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Set getManagedObjects(EnumSet<ObjectState> arg0, Class... arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getMultithreaded() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object getObjectById(Object arg0) {
        checkOpen();
        return _nativeConnection.getObjectsById(arg0)[0];
	}

	public <T> T getObjectById(Class<T> arg0, Object arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object[] getObjectsById(Object[] arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object[] getObjectsById(boolean arg0, Object... arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Sequence getSequence(String arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Date getServerDate() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object getTransactionalObjectId(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object getUserObject() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object getUserObject(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeNontransactional(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeNontransactionalAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeNontransactionalAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public <T> T[] makePersistentAll(T... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public <T> Collection<T> makePersistentAll(Collection<T> arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransactional(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransactionalAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransactionalAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransient(Object arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransientAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransientAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransientAll(Object[] arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransientAll(boolean arg0, Object... arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void makeTransientAll(Collection arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Query newNamedQuery(Class arg0, String arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object newObjectIdInstance(Class arg0, Object arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Query newQuery() {
        checkOpen();
        return new QueryImpl(this);
	}

	public Query newQuery(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Query newQuery(String arg0) {
        checkOpen();
		return new QueryImpl(this, arg0);
	}

	public Query newQuery(Class arg0) {
        checkOpen();
		return new QueryImpl(this, arg0, "");
	}

	public Query newQuery(Extent arg0) {
        checkOpen();
        return new QueryImpl(this, arg0);
	}

	public Query newQuery(String arg0, Object arg1) {
        checkOpen();
		if (arg0.equals(Query.JDOQL)) {
			return newQuery(arg1);
		}
		throw new UnsupportedOperationException("Query type not supported: " + arg0);
	}

	public Query newQuery(Class arg0, Collection arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Query newQuery(Class arg0, String arg1) {
        checkOpen();
        return new QueryImpl(this, arg0, arg1);
	}

	public Query newQuery(Extent arg0, String arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Query newQuery(Class arg0, Collection arg1, String arg2) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object putUserObject(Object arg0, Object arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void refreshAll() {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void refreshAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void refreshAll(JDOException arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void removeInstanceLifecycleListener(InstanceLifecycleListener arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object removeUserObject(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void retrieve(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void retrieve(Object arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void retrieveAll(Collection arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void retrieveAll(Object... arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void retrieveAll(Collection arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void retrieveAll(Object[] arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void retrieveAll(boolean arg0, Object... arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setCopyOnAttach(boolean arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setDetachAllOnCommit(boolean arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setIgnoreCache(boolean arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setMultithreaded(boolean arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setUserObject(Object arg0) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Collection getObjectsById(Collection arg0, boolean arg1) {
        checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
	
	/**
	 * INTERNAL!
	 */
	public Session getSession() {
		return _nativeConnection;
	}
}	
