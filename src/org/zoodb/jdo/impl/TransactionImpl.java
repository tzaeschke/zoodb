/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.jdo.impl;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;
import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.zoodb.internal.DataStoreHandler;
import org.zoodb.internal.Session;
import org.zoodb.internal.util.DBTracer;

/**
 *
 * @author Tilmann Zaeschke
 */
public class TransactionImpl implements Transaction {

    //The final would possibly avoid garbage collection
    private final PersistenceManagerImpl pm;
    private volatile Synchronization sync = null;
    private volatile boolean retainValues = false;
    private volatile boolean optimistic = false;
    
    private final Session connection;

    /**
     * @param pm PersitenceManager
	 * @param retainValues retain values flag
     * @param isOptimistic optimistic flag
	 * @param con current session
     */
    TransactionImpl(PersistenceManagerImpl pm, 
            boolean retainValues, boolean isOptimistic, Session con) {
        DataStoreHandler.connect(null);
        this.retainValues = retainValues;
        this.pm = pm;
        this.connection = con;
        setOptimistic(isOptimistic);
    }

    /**
     * @see Transaction#begin()
     */
    @Override
	public synchronized void begin() {
    	DBTracer.logCall(this);
    	connection.begin();
    }

    /**
     * @see Transaction#commit()
     */
    @Override
	public synchronized void commit() {
    	DBTracer.logCall(this);
    	if (!connection.isActive()) {
    		throw new JDOUserException("Can't commit inactive transaction. Missing 'begin()'?");
    	}

    	//synchronization #1
    	if (sync != null) {
    		sync.beforeCompletion();
    	}

    	//commit
    	connection.commit(retainValues);

    	//synchronization #2
    	if (sync != null) {
    		sync.afterCompletion(Status.STATUS_COMMITTED);
    	}
    }

    /**
     * @see Transaction#commit()
     */
    @Override
	public synchronized void rollback() {
    	DBTracer.logCall(this);
    	if (!connection.isActive()) {
    		throw new JDOUserException("Can't rollback inactive transaction. Missing 'begin()'?");
    	}
    	//Don't call beforeCompletion() here. (JDO 3.0, p153)
    	connection.rollback();
    	if (sync != null) {
    		sync.afterCompletion(Status.STATUS_ROLLEDBACK);
    	}
    }

    /**
     * @see Transaction#getPersistenceManager()
     */
    @Override
	public PersistenceManager getPersistenceManager() {
    	DBTracer.logCall(this);
        //Not synchronised, field is final
        return pm;
    }

    /**
     * @see Transaction#isActive()
     */
    @Override
	public boolean isActive() {
    	DBTracer.logCall(this);
        //Not synchronised, field is volatile
        return connection.isActive();
    }
    
    /**
     * @see Transaction#getSynchronization()
     */@Override
	synchronized 
    public Synchronization getSynchronization() {
     	DBTracer.logCall(this);
       return sync;
    }

    /**
     * @see Transaction#setSynchronization(Synchronization)
     */
    @Override
	public synchronized void setSynchronization(Synchronization sync) {
    	DBTracer.logCall(this);
        this.sync = sync;
    }

	@Override
	public String getIsolationLevel() {
    	DBTracer.logCall(this);
		// TODO Auto-generated method stub
//		javax.jdo.option.TransactionIsolationLevel.read-committed
//		The datastore supports the read-committed isolation level.
//		javax.jdo.option.TransactionIsolationLevel.read-uncommitted
//		The datastore supports the read-uncommitted isolation level.
//		javax.jdo.option.TransactionIsolationLevel.repeatable-read
//		The datastore supports the repeatable-read isolation level.
//		javax.jdo.option.TransactionIsolationLevel.serializable
//		The datastore supports the serializable isolation level.
//		javax.jdo.option.TransactionIsolationLevel.snapshot
//		The datastore supports the snapshot isolation level.	
		return "read-committed";
	}

	@Override
	public boolean getNontransactionalRead() {
    	DBTracer.logCall(this);
		return connection.getConfig().getNonTransactionalRead();
	}

	@Override
	public boolean getNontransactionalWrite() {
    	DBTracer.logCall(this);
		return false;
	}

	@Override
	public boolean getOptimistic() {
    	DBTracer.logCall(this);
		return optimistic;
	}

	@Override
	public boolean getRestoreValues() {
    	DBTracer.logCall(this);
		return false;
	}

	@Override
	public boolean getRetainValues() {
    	DBTracer.logCall(this);
		return retainValues;
	}

	@Override
	public boolean getRollbackOnly() {
    	DBTracer.logCall(this);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setIsolationLevel(String arg0) {
    	DBTracer.logCall(this);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setNontransactionalRead(boolean arg0) {
    	DBTracer.logCall(this);
    	connection.getConfig().setNonTransactionalRead(arg0);
	}

	@Override
	public void setNontransactionalWrite(boolean arg0) {
    	DBTracer.logCall(this);
		if (arg0 == true) {
			throw new UnsupportedOperationException("Nontransactional write is supported");
		}
	}

	@Override
	public void setOptimistic(boolean arg0) {
    	DBTracer.logCall(this);
    	optimistic = arg0;
    	if (arg0) {
    		throw new UnsupportedOperationException();
    	}
	}

	@Override
	public void setRestoreValues(boolean arg0) {
    	DBTracer.logCall(this);
		// restore values after rollback?
		if (arg0 == true) {
			throw new UnsupportedOperationException(
					"Restoring values after rollback is not supported");
		}
	}

	@Override
	public void setRetainValues(boolean arg0) {
    	DBTracer.logCall(this);
		// retain values after commit?
		retainValues = arg0;
	}

	@Override
	public void setRollbackOnly() {
    	DBTracer.logCall(this);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setSerializeRead(Boolean serialize) {
    	DBTracer.logCall(this);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	@Override
	public Boolean getSerializeRead() {
    	DBTracer.logCall(this);
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}
}
