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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.jdo.FetchGroup;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.datastore.DataStoreCache;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.jdo.metadata.JDOMetadata;
import javax.jdo.metadata.TypeMetadata;
import javax.jdo.spi.JDOImplHelper;
import javax.jdo.spi.StateInterrogation;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.jdo.spi.ZooStateInterrogator;

/**
 * This class simulates the JDO PersistenceManagerFactory
 *
 * @author Tilmann Zaeschke
 */
public class PersistenceManagerFactoryImpl 
        extends AbstractPersistenceManagerFactory {

	private static final long serialVersionUID = 1L;
	private Set<PersistenceManagerImpl> pms = new HashSet<>();
	private boolean isClosed = false;
	private boolean isReadOnly = false;
	private static final StateInterrogation SI = new ZooStateInterrogator();
	
	private HashMap<InstanceLifecycleListener, List<Class<?>>> lcListeners = new HashMap<>(); 
	
    /**
     * @param props NOT SUPPORTED!
     */
    public PersistenceManagerFactoryImpl(Properties props) {
        super(props);
        JDOImplHelper.getInstance().addStateInterrogation(SI);
    }

    /**
     * Not in standard, but required in Poleposition Benchmark / JDO 1.0.2
     * @param props The properties
     * @return new PersistenceManagerFactory
     */
    public static PersistenceManagerFactory getPersistenceManagerFactory (Properties
    		props) {
    	return new PersistenceManagerFactoryImpl(props);
    }
    public static PersistenceManagerFactory getPersistenceManagerFactory (Map<?, ?>
    		props) {
    	return new PersistenceManagerFactoryImpl((Properties) props);
    }
	public static PersistenceManagerFactory getPersistenceManagerFactory (Map<?, ?>
    		overrides, Map<?, ?> props) {
		System.err.println("STUB PersistenceManagerFactoryImpl." +
				"getPersistenceManagerFactory(o, p)");
    	return new PersistenceManagerFactoryImpl((Properties) props);
	}

    
    /**
     * @see PersistenceManagerFactory#getPersistenceManager()
     */
	@Override
    public PersistenceManager getPersistenceManager() {
    	checkOpen();
        PersistenceManagerImpl pm = new PersistenceManagerImpl(this, getConnectionPassword());
		synchronized (pms) {
			pms.add(pm);
		}
        setFrozen();
        
        //init
        for (Map.Entry<InstanceLifecycleListener, List<Class<?>>> e: lcListeners.entrySet()) {
        	for (Class<?> c: e.getValue()) {
        		pm.getSession().addInstanceLifecycleListener(e.getKey(), new Class[]{c});
        		//pm.addInstanceLifecycleListener(e.getKey(), c);
        	}
        }
        
        return pm;
    }
    
    /**
     * @see PersistenceManagerFactory#getProperties()
     */
	@Override
    public Properties getProperties() {
        //return null;
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
    }
    
	@Override
    public Object clone() {
        PersistenceManagerFactoryImpl pmf = 
            (PersistenceManagerFactoryImpl) super.clone();
        pmf.pms = new HashSet<PersistenceManagerImpl>(); //do not clone _pm!
        return pmf;
    }

	@Override
	public void addFetchGroups(FetchGroup... arg0) {
		checkOpen(); //? TZ
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void addInstanceLifecycleListener(InstanceLifecycleListener arg0, Class[] arg1) {
		checkFrozen();
		List<Class<?>> clsL = lcListeners.get(arg0);
		if (clsL == null) {
			clsL = new LinkedList<Class<?>>();
			lcListeners.put(arg0, clsL);
		}
		if (arg1 != null) {
			for (Class<?> c: arg1) {
				clsL.add(c);
			}
		} else {
			clsL.add(ZooPC.class);
		}
	}

	@Override
	public void close() {
		//Close this PersistenceManagerFactory. Check for JDOPermission(
		//"closePersistenceManagerFactory") and if not authorized, throw SecurityException.
		//If the authorization check succeeds, check to see that all PersistenceManager instances 
		//obtained from this PersistenceManagerFactory have no active transactions. If any 
		//PersistenceManager instances have an active transaction, throw a JDOUserException, with 
		//one nested JDOUserException for each PersistenceManager with an active Transaction.
		//If there are no active transactions, then close all PersistenceManager instances obtained 
		//from this	PersistenceManagerFactory and mark this PersistenceManagerFactory as closed. 
		//After close completes, disallow all methods except close, isClosed, and get methods 
		//except for getPersistenceManager.
		//If any disallowed method is called after close, then JDOUserException is thrown.
		//TODO fix!
		synchronized (pms) {
			for (PersistenceManagerImpl pm: pms) {
				if (!pm.isClosed() && pm.currentTransaction().isActive()) {
					throw new JDOUserException("Found active PersistenceManager. ", 
							new JDOUserException(), pm);
				}
			}
			while(!pms.isEmpty()) {
				PersistenceManager pm = pms.iterator().next();
				if (!pm.isClosed()) {
					pm.close();
				} else {
					//This is a contingency measure for failing TX
					pms.remove(pm);
				}
			}
		}
        JDOImplHelper.getInstance().removeStateInterrogation(SI);
		isClosed = true;
	}

	@Override
	public String getConnectionDriverName() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getConnectionFactory() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Object getConnectionFactory2() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public String getConnectionFactory2Name() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public String getConnectionFactoryName() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getCopyOnAttach() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public DataStoreCache getDataStoreCache() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public FetchGroup getFetchGroup(Class arg0, String arg1) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Set getFetchGroups() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public String getMapping() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public String getName() {
		return getSessionName();
	}

	@Override
	public boolean getNontransactionalWrite() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public PersistenceManager getPersistenceManager(String arg0,
			String arg1) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public PersistenceManager getPersistenceManagerProxy() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getReadOnly() {
		return isReadOnly;
	}

	@Override
	public boolean getRestoreValues() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public String getServerTimeZoneID() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public String getTransactionIsolationLevel() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public String getTransactionType() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public void removeAllFetchGroups() {
		checkOpen(); //? TZ
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeFetchGroups(FetchGroup... arg0) {
		checkOpen(); //? TZ
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void removeInstanceLifecycleListener(InstanceLifecycleListener arg0) {
		checkFrozen();
		lcListeners.remove(arg0);
	}

	@Override
	public void setConnectionDriverName(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setConnectionFactory(Object arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setConnectionFactory2(Object arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setConnectionFactory2Name(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setConnectionFactoryName(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCopyOnAttach(boolean arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMapping(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setName(String arg0) {
		checkOpen();
		setSessionName(arg0);
	}

	@Override
	public void setNontransactionalWrite(boolean arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setReadOnly(boolean arg0) {
		checkOpen();
		isReadOnly = arg0;	
	}

	@Override
	public void setRestoreValues(boolean arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setServerTimeZoneID(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTransactionIsolationLevel(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public void setTransactionType(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<String> supportedOptions() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
	
	private void checkOpen() {
		if (isClosed) {
			throw new JDOUserException("The Factory is already closed.");
		}
	}

	void deRegister(PersistenceManagerImpl persistenceManagerImpl) {
		synchronized (pms) {
			pms.remove(persistenceManagerImpl);
		}
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
	public TypeMetadata getMetadata(String arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public JDOMetadata newMetadata() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public void registerMetadata(JDOMetadata arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
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

	@Override
	public Collection<Class> getManagedClasses() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}
}
