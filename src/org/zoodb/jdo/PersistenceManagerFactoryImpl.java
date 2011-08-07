package org.zoodb.jdo;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.jdo.FetchGroup;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.datastore.DataStoreCache;
import javax.jdo.listener.InstanceLifecycleListener;

/**
 * This class simulates the JDO PersistenceManagerFactory
 *
 * @author Tilmann Zaeschke
 */
public class PersistenceManagerFactoryImpl 
        extends AbstractPersistenceManagerFactory {

	private static final long serialVersionUID = 1L;
	private Set<PersistenceManagerImpl> _pms = new HashSet<PersistenceManagerImpl>();
	private boolean _isClosed = false;
	private String _name;
	private boolean _isReadOnly = false;
	
    /**
     * @param props NOT SUPPORTED!
     */
    public PersistenceManagerFactoryImpl(Properties props) {
        super(props);
    }

    /**
     * Not in standard, but required in Poleposition Benchmark / JDO 1.0.2
     * @param props
     * @return
     */
    public static PersistenceManagerFactory getPersistenceManagerFactory (Properties
    		props) {
    	return new PersistenceManagerFactoryImpl(props);
    }
    public static PersistenceManagerFactory getPersistenceManagerFactory (Map
    		props) {
    	return new PersistenceManagerFactoryImpl((Properties) props);
    }
	public static PersistenceManagerFactory getPersistenceManagerFactory (Map
    		overrides, Map props) {
		System.err.println("STUB PersistenceManagerFactoryImpl." +
				"getPersistenceManagerFactory(o, p)");
    	return new PersistenceManagerFactoryImpl((Properties) props);
	}

    
    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManagerFactory
     * #getPersistenceManager()
     */
    public PersistenceManager getPersistenceManager() {
    	checkOpen();
    	if (!_pms.isEmpty()) {
    		//TODO
    		System.err.println("WARNING Multiple PM per factory are not supported.");
    	    //throw new UnsupportedOperationException("Multiple PM per factory are not supported.");
    	}
        PersistenceManagerImpl pm = new PersistenceManagerImpl(this, getConnectionPassword());
        _pms.add(pm);
        setFrozen();
        return pm;
    }
    
    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManagerFactory
     * #getProperties()
     */
    public Properties getProperties() {
        //return null;
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
    }
    
    public Object clone() {
        PersistenceManagerFactoryImpl pmf = 
            (PersistenceManagerFactoryImpl) super.clone();
        pmf._pms = new HashSet<PersistenceManagerImpl>(); //do not clone _pm!
        return pmf;
    }

	public void addFetchGroups(FetchGroup... arg0) {
		checkOpen(); //? TZ
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void addInstanceLifecycleListener(InstanceLifecycleListener arg0,
			Class[] arg1) {
		checkOpen(); //? TZ
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void close() {
		for (PersistenceManagerImpl pm: _pms) {
			if (!pm.isClosed()) {
				throw new JDOUserException("Found open PersistenceManager. ", 
						new JDOUserException(), pm);
			}
		}
		_isClosed = true;
	}

	public String getConnectionDriverName() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object getConnectionFactory() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Object getConnectionFactory2() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public String getConnectionFactory2Name() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public String getConnectionFactoryName() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getCopyOnAttach() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public DataStoreCache getDataStoreCache() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getDetachAllOnCommit() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public FetchGroup getFetchGroup(Class arg0, String arg1) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Set getFetchGroups() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public String getMapping() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public String getName() {
		return _name;
	}

	public boolean getNontransactionalWrite() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public javax.jdo.PersistenceManager getPersistenceManager(String arg0,
			String arg1) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public javax.jdo.PersistenceManager getPersistenceManagerProxy() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public String getPersistenceUnitName() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean getReadOnly() {
		return _isReadOnly;
	}

	public boolean getRestoreValues() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public String getServerTimeZoneID() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public String getTransactionIsolationLevel() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public String getTransactionType() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public boolean isClosed() {
		return _isClosed;
	}

	public void removeAllFetchGroups() {
		checkOpen(); //? TZ
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void removeFetchGroups(FetchGroup... arg0) {
		checkOpen(); //? TZ
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void removeInstanceLifecycleListener(InstanceLifecycleListener arg0) {
		checkOpen(); //? TZ
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setConnectionDriverName(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setConnectionFactory(Object arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setConnectionFactory2(Object arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setConnectionFactory2Name(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setConnectionFactoryName(String arg0) {
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

	public void setMapping(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setName(String arg0) {
		checkOpen();
		_name = arg0;
	}

	public void setNontransactionalRead(boolean arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setNontransactionalWrite(boolean arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setPersistenceUnitName(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setReadOnly(boolean arg0) {
		checkOpen();
		_isReadOnly = arg0;	
	}

	public void setRestoreValues(boolean arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setServerTimeZoneID(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setTransactionIsolationLevel(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public void setTransactionType(String arg0) {
		checkOpen();
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	public Collection<String> supportedOptions() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
	
	private void checkOpen() {
		if (_isClosed) {
			throw new JDOUserException("The Factory is already closed.");
		}
	}

	void deRegister(PersistenceManagerImpl persistenceManagerImpl) {
		_pms.remove(persistenceManagerImpl);
	}
}
