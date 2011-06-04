package org.zoodb.jdo;

import java.util.Properties;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;
import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.zoodb.jdo.internal.DataStoreHandler;
import org.zoodb.jdo.internal.Session;

/**
 *
 * @author Tilmann Zaeschke
 */
public class TransactionImpl implements Transaction {

    private volatile boolean _isOpen = false;
    //The final would possibly avoid garbage collection
    private final PersistenceManagerImpl _pm;
    private Synchronization _sync = null;
    private volatile boolean _retainValues = false;
    private volatile boolean _dropRLockSession = false;
    private volatile boolean _useDefaultSessions = false;
    
    private final Session _connection;

    /**
     * @param arg0
     * @param pm
     * @param i 
     */
    TransactionImpl(Properties arg0, PersistenceManagerImpl pm, 
            boolean retainValues, boolean isOptimistic, 
            boolean isAutoJoinThreads, Session con) {
        DataStoreHandler.connect(arg0);
        _retainValues = retainValues;
        _useDefaultSessions = isAutoJoinThreads;
        //Always use DROP_RLOCK when NOLOCK is used (= optimistic session).
        _dropRLockSession = isOptimistic;
        _pm = pm;
        _connection = con;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#begin()
     */
    public synchronized void begin() {
        if (_isOpen) {
            throw new JDOUserException(
                    "Can't open new transaction inside existing transaction.");
        }
        _isOpen = true;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#commit()
     */
    public synchronized void commit() {
    	if (!_isOpen) {
    		throw new JDOUserException("Can't commit closed " +
    		"transaction. Missing 'begin()'?");
    	}

    	//synchronisation #1
    	_isOpen = false;
    	if (_sync != null) {
    		_sync.beforeCompletion();
    	}

    	//commit
    	_connection.commit(_retainValues);

    	//synchronization #2
    	if (_sync != null) {
    		_sync.afterCompletion(Status.STATUS_COMMITTED);
    	}
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#commit()
     */
    public synchronized void rollback() {
    	if (!_isOpen) {
    		throw new JDOUserException("Can't rollback closed " +
    		"transaction. Missing 'begin()'?");
    	}
    	_isOpen = false;
    	//Don't call beforeCompletion() here.
    	_connection.rollback();
    	if (_sync != null) {
    		_sync.afterCompletion(Status.STATUS_ROLLEDBACK);
    	}
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#getPersistenceManager()
     */
    public PersistenceManager getPersistenceManager() {
        //Not synchronised, field is final
        return _pm;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#isActive()
     */
    public boolean isActive() {
        //Not synchronised, field is volatile
        return _isOpen;
    }
    
    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#getSynchronization()
     */synchronized 
    public Synchronization getSynchronization() {
        return _sync;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#setSynchronization(
     * javax.Transaction.Synchronization)
     */
    public synchronized void setSynchronization(Synchronization sync) {
        _sync = sync;
    }

	public String getIsolationLevel() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getNontransactionalRead() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getNontransactionalWrite() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getOptimistic() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getRestoreValues() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getRetainValues() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getRollbackOnly() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setIsolationLevel(String arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setNontransactionalRead(boolean arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setNontransactionalWrite(boolean arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setOptimistic(boolean arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setRestoreValues(boolean arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setRetainValues(boolean arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setRollbackOnly() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
}
