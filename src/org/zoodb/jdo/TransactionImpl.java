/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;
import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.zoodb.jdo.internal.DataStoreHandler;
import org.zoodb.jdo.internal.Session;
import org.zoodb.profiling.api.impl.ProfilingManager;

/**
 *
 * @author Tilmann Zaeschke
 */
public class TransactionImpl implements Transaction {

    private volatile boolean isOpen = false;
    //The final would possibly avoid garbage collection
    private final PersistenceManagerImpl pm;
    private volatile Synchronization sync = null;
    private volatile boolean retainValues = false;
    
    private final Session connection;
    
    private long trxId = 0;

    /**
     * @param arg0
     * @param pm
     * @param i 
     */
    TransactionImpl(PersistenceManagerImpl pm, 
            boolean retainValues, boolean isOptimistic, Session con) {
        DataStoreHandler.connect(null);
        this.retainValues = retainValues;
        this.pm = pm;
        this.connection = con;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#begin()
     */
    public synchronized void begin() {
        if (isOpen) {
            throw new JDOUserException(
                    "Can't open new transaction inside existing transaction.");
        }
        isOpen = true;
        trxId++;
        ProfilingManager.getInstance().newTrxEvent(this);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#commit()
     */
    public synchronized void commit() {
    	if (!isOpen) {
    		throw new JDOUserException("Can't commit closed " +
    		"transaction. Missing 'begin()'?");
    	}

    	//synchronisation #1
    	if (sync != null) {
    		sync.beforeCompletion();
    	}

    	//commit
    	connection.commit(retainValues);
    	isOpen = false;

    	//synchronization #2
    	if (sync != null) {
    		sync.afterCompletion(Status.STATUS_COMMITTED);
    	}
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#commit()
     */
    public synchronized void rollback() {
    	if (!isOpen) {
    		throw new JDOUserException("Can't rollback closed " +
    		"transaction. Missing 'begin()'?");
    	}
    	//Don't call beforeCompletion() here. (JDO 3.0, p153)
    	connection.rollback();
    	isOpen = false;
    	if (sync != null) {
    		sync.afterCompletion(Status.STATUS_ROLLEDBACK);
    	}
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#getPersistenceManager()
     */
    public PersistenceManager getPersistenceManager() {
        //Not synchronised, field is final
        return pm;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#isActive()
     */
    public boolean isActive() {
        //Not synchronised, field is volatile
        return isOpen;
    }
    
    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#getSynchronization()
     */synchronized 
    public Synchronization getSynchronization() {
        return sync;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Transaction#setSynchronization(
     * javax.Transaction.Synchronization)
     */
    public synchronized void setSynchronization(Synchronization sync) {
        this.sync = sync;
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

	@Override
	public Boolean getSerializeRead() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSerializeRead(Boolean arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public long getTrxId() {
		return trxId;
	}
	
	/**
	 * If there are multiple PersistenceManagers, the trxId itself will not be globally unique.
	 * Use the pair (persistenceManagerId,trxId) as a unique identifier
	 * @return
	 */
	public String getUniqueTrxId() {
		return pm.getUniqueId() + "." + trxId; 
	}
}
