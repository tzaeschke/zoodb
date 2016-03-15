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

import java.util.Properties;

import javax.jdo.Constants;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.api.ZooConstants;
import org.zoodb.tools.ZooHelper;


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
    private boolean isOptimistic = false;
    private boolean isRetainValues = false;
    private boolean isDetachAllOnCommit = false;
    //should be 'false' by default
    private boolean isIgnoreCache = false;
    private boolean isMultiThreaded = false;
    private String userName = null;
    private transient String password = null;
    private String database = null;

    private boolean nonTransactionalRead = false;
    private boolean autoCreateSchema = true;
	private boolean evictPrimitives = false;
//	private boolean allowNonStandardSCOs = false;
    
    //Non-standard properties.
    //private boolean _isReadOnly = false; //now in JDO 2.2
	private String sessionName = null;
	private String persistenceUnitName = null;
    private int oidAllocation = 50;
    
    private boolean isFrozen = false;


    /**
     * @param props
     */
    public AbstractPersistenceManagerFactory(Properties props) {
    	/*
    	- "javax.jdo.option.Optimistic"
    	- "javax.jdo.option.RetainValues"
    	- "javax.jdo.option.RestoreValues"
    	- "javax.jdo.option.IgnoreCache"
    	- "javax.jdo.option.NontransactionalRead"
    	- "javax.jdo.option.NontransactionalWrite"
    	- "javax.jdo.option.Multithreaded"
    	- "javax.jdo.option.DetachAllOnCommit"
    	- "javax.jdo.option.CopyOnAttach"
    	- "javax.jdo.option.ConnectionUserName"
    	- "javax.jdo.option.ConnectionPassword"
    	- "javax.jdo.option.ConnectionURL"
    	- "javax.jdo.option.ConnectionDriverName"
    	- "javax.jdo.option.ConnectionFactoryName"
    	- "javax.jdo.option.ConnectionFactory2Name"
    	- "javax.jdo.option.Mapping"
    	- "javax.jdo.mapping.Catalog"
    	- "javax.jdo.mapping.Schema"
    	- "javax.jdo.option.TransactionType"
    	- "javax.jdo.option.ServerTimeZoneID"
    	- "javax.jdo.option.ReadOnly"
    	The following two properties are only used in the props, not in the overrides.
    	- "javax.jdo.option.Name"
    	- "javax.jdo.option.PersistenceUnitName"
    	The property "javax.jdo.PersistenceManagerFactoryClass"
    	*/
    	for (Object keyO: props.keySet()) {
    		String key = (String) keyO;
    		if (Constants.PROPERTY_OPTIMISTIC.equals(key)) {
    			isOptimistic = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_RETAIN_VALUES.equals(key)) {
    			isRetainValues = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_RESTORE_VALUES.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_IGNORE_CACHE.equals(key)) {
    			isIgnoreCache = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_NONTRANSACTIONAL_READ.equals(key)) {
    			nonTransactionalRead = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_NONTRANSACTIONAL_WRITE.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_MULTITHREADED.equals(key)) {
    			isMultiThreaded = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_DETACH_ALL_ON_COMMIT.equals(key)) {
    			isDetachAllOnCommit = Boolean.parseBoolean(props.getProperty(key));
    		} else if (Constants.PROPERTY_COPY_ON_ATTACH.equals(key)) {
    			System.out.println("STUB: Property not supported: " + key + "=" + props.get(key)); //TODO
    		} else if (Constants.PROPERTY_CONNECTION_USER_NAME.equals(key)) {
    			userName = props.getProperty(key);
    		} else if (Constants.PROPERTY_CONNECTION_PASSWORD.equals(key)) {
    			password = props.getProperty(key);
    		} else if (Constants.PROPERTY_CONNECTION_URL.equals(key)) {
    			database = props.getProperty(key);
    			//This is necessary, because the database name may have been set via XML or
    			//manually via properties.
    			database = ZooHelper.getDataStoreManager().getDbPath(database);
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
    			sessionName = props.getProperty(key);
    		} else if (Constants.PROPERTY_PERSISTENCE_UNIT_NAME.equals(key)) {
    			persistenceUnitName = props.getProperty(key);
//    		} else if ("options".equals(key)) {
//    			String opt = props.getProperty(key);
//    			if ("0".equals(opt)) {
//    				
//    			} else {
//    				throw new IllegalArgumentException("o=" + opt);
//    			}
    		} else if (Constants.PROPERTY_PERSISTENCE_MANAGER_FACTORY_CLASS.equals(key)) {
    			//ignore
//    		} else if (ZooConstants.PROPERTY_ALLOW_NON_STANDARD_SCOS.equals(key)) {
//    			allowNonStandardSCOs = Boolean.parseBoolean(props.getProperty(key));
    		} else if (ZooConstants.PROPERTY_AUTO_CREATE_SCHEMA.equals(key)) {
    			autoCreateSchema = Boolean.parseBoolean(props.getProperty(key));
    		} else if (ZooConstants.PROPERTY_EVICT_PRIMITIVES.equals(key)) {
    			evictPrimitives = Boolean.parseBoolean(props.getProperty(key));
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
        isFrozen = true;
    }

    /**
     * Throws a JDOUserException if this factories properties are frozen. 
     * Should be called from every set-method.
     */
    protected void checkFrozen() {
        if (isFrozen) {
            //TODO is this the correct exception?
            throw new JDOUserException("This factory can't be modified.");
        }
    }
    
    /**
     * @see PersistenceManagerFactory#getRetainValues()
     */
    @Override
    public boolean getRetainValues() {
        return isRetainValues;
    }

    /**
     * @see PersistenceManagerFactory#setRetainValues(boolean)
     */
    @Override
    public void setRetainValues(boolean flag) {
        checkFrozen();
        isRetainValues = flag;
    }

    @Override
    public String getConnectionUserName() {
        return userName;
    }

    protected String getConnectionPassword() {
        return password;
    }

	@Override
	public boolean getDetachAllOnCommit() {
		return isDetachAllOnCommit;
	}

	@Override
	public void setDetachAllOnCommit(boolean arg0) {
		checkFrozen();
		this.isDetachAllOnCommit = arg0;
	}

	@Override
    public boolean getOptimistic() {
        return isOptimistic;
    }

    @Override
    public boolean getNontransactionalRead() {
        return nonTransactionalRead;
    }

	@Override
	public void setNontransactionalRead(boolean arg0) {
		checkFrozen();
		nonTransactionalRead = arg0;
	}

    @Override
    public void setConnectionPassword(String password) {
        checkFrozen();
        this.password = password;
    }

    @Override
    public void setConnectionUserName(String userName) {
        checkFrozen();
        this.userName = userName;
    }

    @Override
    public void setOptimistic(boolean flag) {
        checkFrozen();
        this.isOptimistic = flag;
    }

    @Override
    public String getConnectionURL() {
        return database;
    }

    @Override
    public void setConnectionURL(String url) {
        checkFrozen();
        this.database = url;
    }

	@Override
	public String getPersistenceUnitName() {
		return persistenceUnitName;
	}

	@Override
	public void setPersistenceUnitName(String arg0) {
		checkFrozen();
		this.persistenceUnitName = arg0;
	}

	public String getSessionName() {
        return sessionName;
    }

    public void setSessionName(String sessionName) {
        checkFrozen();
        this.sessionName = sessionName;
    }

    public int getOidAllocation() {
        return oidAllocation;
    }

    public void setOidAllocation(int size) {
        checkFrozen(); //is this check required?
        this.oidAllocation = size;
    }
    
	@Override
	public boolean getIgnoreCache() {
		return isIgnoreCache;
	}

	@Override
	public boolean getMultithreaded() {
		return isMultiThreaded;
	}
	
	@Override
	public void setIgnoreCache(boolean arg0) {
		//TODO
		this.isIgnoreCache = arg0;
		if (!isIgnoreCache)
			System.out.println("STUB: IgnoreCache = false not supported."); //TODO
	}

	@Override
	public void setMultithreaded(boolean arg0) {
		this.isMultiThreaded = arg0;
	}

    @Override
	public Object clone() {
        AbstractPersistenceManagerFactory obj;
        try {
            obj = (AbstractPersistenceManagerFactory) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }

        obj.isFrozen = false; //Allow modification of cloned object
        obj.setConnectionPassword(getConnectionPassword());
        obj.setConnectionURL(getConnectionURL());
        obj.setConnectionUserName(getConnectionUserName());
        obj.setOidAllocation(getOidAllocation());
        obj.setOptimistic(getOptimistic());
        obj.setReadOnly(getReadOnly());
        obj.setRetainValues(getRetainValues());
        obj.setSessionName(null); //Force creation of a new name
        obj.setIgnoreCache(obj.getIgnoreCache());
        obj.setDetachAllOnCommit(getDetachAllOnCommit());
        obj.setMultithreaded(getMultithreaded());
        return obj;
    }
    
    
//	public boolean getAllowNonStandardSCOs() {
//		return allowNonStandardSCOs;
//	}

	
	public boolean getAutoCreateSchema() {
		return autoCreateSchema;
	}
    

	public boolean getEvictPrimitives() {
		return evictPrimitives;
	}
}
