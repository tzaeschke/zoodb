package org.zoodb.jdo;

import java.util.Properties;

import javax.jdo.Constants;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManagerFactory;


/**
 * This class simulates the JDO PersistenceManagerFactory
 *
 * @author Tilmann Zaeschke
 */
public abstract class AbstractPersistenceManagerFactory 
        implements PersistenceManagerFactory, Cloneable {

    //See JDO 2.0 Chapter 11, p87
//  /** <code>OPTION_CONNECTION_URL</code> = 
//   * "javax.jdo.option.ConnectionURL"*/
//public static final String OPTION_CONNECTION_URL = 
//      "javax.jdo.option.ConnectionURL";

//  /** <code>OPTION_RETAIN_VALUES</code> = 
//   * "javax.jdo.option.RetainValues"*/
//  public static final String OPTION_RETAIN_VALUES = 
//      "javax.jdo.option.RetainValues";
  
//  public static final String OPTION_USERNAME = 
//  "javax.jdo.option.ConnectionUserName";
//  public static final String OPTION_PASSWORD = 
//  "javax.jdo.option.ConnectionPassword";

	private static final long serialVersionUID = 1L;

	
	//standard properties
    private boolean _isOptimistic = false;
    private boolean _isRetainValues = false;
    //TODO should be 'false' by default
    private boolean _isIgnoreCache = false;
    private boolean _isMultiThreaded = false;
    private String _userName = null;
    private transient String _password = null;
    private String _database = null;

    private boolean _nonTransactionalRead = false;
    private boolean _autoCreateSchema = false;
    
    //Non-standard properties.
    //private boolean _isReadOnly = false; //now in JDO 2.2
    private String _sessionName = null;
    private int _oidAllocation = 50;
    
    private boolean _isFrozen = false;

    /**
     * @param props
     */
    public AbstractPersistenceManagerFactory(Properties props) {
    	/*
    	• "javax.jdo.option.Optimistic"
    	• "javax.jdo.option.RetainValues"
    	• "javax.jdo.option.RestoreValues"
    	• "javax.jdo.option.IgnoreCache"
    	• "javax.jdo.option.NontransactionalRead"
    	• "javax.jdo.option.NontransactionalWrite"
    	• "javax.jdo.option.Multithreaded"
    	• "javax.jdo.option.DetachAllOnCommit"
    	• "javax.jdo.option.CopyOnAttach"
    	• "javax.jdo.option.ConnectionUserName"
    	• "javax.jdo.option.ConnectionPassword"
    	• "javax.jdo.option.ConnectionURL"
    	• "javax.jdo.option.ConnectionDriverName"
    	• "javax.jdo.option.ConnectionFactoryName"
    	• "javax.jdo.option.ConnectionFactory2Name"
    	• "javax.jdo.option.Mapping"
    	• "javax.jdo.mapping.Catalog"
    	• "javax.jdo.mapping.Schema"
    	• "javax.jdo.option.TransactionType"
    	• "javax.jdo.option.ServerTimeZoneID"
    	• "javax.jdo.option.ReadOnly"
    	The following two properties are only used in the props, not in the overrides.
    	• "javax.jdo.option.Name"
    	• "javax.jdo.option.PersistenceUnitName"
    	The property "javax.jdo.PersistenceManagerFactoryClass"
    	*/
    	for (Object keyO: props.keySet()) {
    		String key = (String) keyO;
    		if (Constants.PROPERTY_OPTIMISTIC.equals(key)) {
    			_isOptimistic = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_RETAIN_VALUES.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_RESTORE_VALUES.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_IGNORE_CACHE.equals(key)) {
    			_isIgnoreCache = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_NONTRANSACTIONAL_READ.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    			_nonTransactionalRead = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_NONTRANSACTIONAL_WRITE.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_MULTITHREADED.equals(key)) {
    			_isMultiThreaded = Boolean.parseBoolean(props.getProperty(key));
    			if (_isMultiThreaded == true) 
    				System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_DETACH_ALL_ON_COMMIT.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_COPY_ON_ATTACH.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_CONNECTION_USER_NAME.equals(key)) {
    			_userName = props.getProperty(key);
    		} else if (Constants.PROPERTY_CONNECTION_PASSWORD.equals(key)) {
    			_password = props.getProperty(key);
    		} else if (Constants.PROPERTY_CONNECTION_URL.equals(key)) {
    			_database = props.getProperty(key);
    		} else if (Constants.PROPERTY_CONNECTION_DRIVER_NAME.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_CONNECTION_FACTORY_NAME.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_CONNECTION_FACTORY2_NAME.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_MAPPING.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_MAPPING_CATALOG.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_MAPPING_SCHEMA.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if ("javax.jdo.option.TransactionType".equals(key)) { 
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_SERVER_TIME_ZONE_ID.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_READONLY.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    			
    		//The following two properties are only used in the props, not in the overrides.
    		} else if (Constants.PROPERTY_NAME.equals(key)) {
    			_sessionName = props.getProperty(key);
    		} else if (Constants.PROPERTY_PERSISTENCE_UNIT_NAME.equals(key)) {
    			_sessionName = props.getProperty(key);
//    		} else if ("options".equals(key)) {
//    			String opt = props.getProperty(key);
//    			if ("0".equals(opt)) {
//    				
//    			} else {
//    				throw new IllegalArgumentException("o=" + opt);
//    			}
    		} else if (Constants.PROPERTY_PERSISTENCE_MANAGER_FACTORY_CLASS.equals(key)) {
    			//ignore
    		} else if (ZooConstants.PROPERTY_AUTO_CREATE_SCHEMA.equals(key)) {
    			_autoCreateSchema = Boolean.parseBoolean(props.getProperty(key));
    		} else {
    			//throw new IllegalArgumentException("Unknown key: " + key);
    			System.err.println("Property not recognised: " + key + "=" + props.getProperty(key));
    		}
    	}
    }
    
    /**
     * Freezes the properties of this factory. Should be called from any method
     * that creates a PersistenceManager.
     */
    protected void setFrozen() {
        _isFrozen = true;
    }

    /**
     * Throws a JDOUserException if this factories properties are frozen. 
     * Should be called from every set-method.
     */
    protected void checkFrozen() {
        if (_isFrozen) {
            //TODO is this the correct exception?
            throw new JDOUserException("This factory can't be modified.");
        }
    }
    
    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManagerFactory
     * #getRetainValues()
     */
    @Override
    public boolean getRetainValues() {
        return _isRetainValues;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.PersistenceManagerFactory
     * #setRetainValues(boolean)
     */
    @Override
    public void setRetainValues(boolean flag) {
        checkFrozen();
        _isRetainValues = flag;
    }

    @Override
    public String getConnectionUserName() {
        return _userName;
    }

    protected String getConnectionPassword() {
        return _password;
    }

    @Override
    public boolean getOptimistic() {
        return _isOptimistic;
    }

    @Override
    public boolean getNontransactionalRead() {
        return _nonTransactionalRead;
    }

    @Override
    public void setConnectionPassword(String password) {
        checkFrozen();
        _password = password;
    }

    @Override
    public void setConnectionUserName(String userName) {
        checkFrozen();
        _userName = userName;
    }

    @Override
    public void setOptimistic(boolean flag) {
        checkFrozen();
        _isOptimistic = flag;
    }

    @Override
    public String getConnectionURL() {
        return _database;
    }

    @Override
    public void setConnectionURL(String url) {
        checkFrozen();
        _database = url;
    }

    public String getSessionName() {
        return _sessionName;
    }

    public void setSessionName(String sessionName) {
        checkFrozen();
        _sessionName = sessionName;
    }

    public int getOidAllocation() {
        return _oidAllocation;
    }

    public void setOidAllocation(int size) {
        checkFrozen(); //is this check required?
        _oidAllocation = size;
    }
    
	public boolean getIgnoreCache() {
		return _isIgnoreCache;
	}

	public boolean getMultithreaded() {
		return _isMultiThreaded;
	}
	
	public void setIgnoreCache(boolean arg0) {
		//TODO
		_isIgnoreCache = arg0;
		if (!_isIgnoreCache)
			System.out.println("STUB: IgnoreCache = false not supported."); //TODO
	}

	public void setMultithreaded(boolean arg0) {
		//TODO
		_isMultiThreaded = arg0;
		if (_isMultiThreaded == true) 
			System.out.println("STUB: MultiThreaded = true not supported."); //TODO
	}

    public Object clone() {
        AbstractPersistenceManagerFactory obj;
        try {
            obj = (AbstractPersistenceManagerFactory) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }

        obj._isFrozen = false; //Allow modification of cloned object
        obj.setConnectionPassword(getConnectionPassword());
        obj.setConnectionURL(getConnectionURL());
        obj.setConnectionUserName(getConnectionUserName());
        obj.setOidAllocation(getOidAllocation());
        obj.setOptimistic(getOptimistic());
        obj.setReadOnly(getReadOnly());
        obj.setRetainValues(getRetainValues());
        obj.setSessionName(null); //Force creation of a new name
        return obj;
    }
    

	public boolean getAutoCreateSchema() {
		return _autoCreateSchema;
	}
}
