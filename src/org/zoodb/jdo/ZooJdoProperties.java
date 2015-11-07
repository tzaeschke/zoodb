/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

import java.util.Properties;

import javax.jdo.Constants;

import org.zoodb.api.ZooConstants;
import org.zoodb.internal.util.DBTracer;
import org.zoodb.jdo.impl.PersistenceManagerFactoryImpl;
import org.zoodb.tools.ZooHelper;
import org.zoodb.tools.impl.DataStoreManager;

/**
 * Properties to be used for creating JDO session.
 * <p>
 * <code>
 * ZooJdoProperties props = new ZooJdoProperties("MyDatabase");  <br> 
 * JDOHelper.getPersistenceManagerFactory(props);    <br>
 * </code>
 * 
 * The default is to use optimistic transactions.
 * 
 * @author Tilmann Zaeschke
 */
public class ZooJdoProperties extends Properties implements Constants {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    /**
     * Creates a new set of properties for creating a new persistence manager. 
     * 
     * If the dbName is a simple file name, the database will be assumed to be in the default
     * folder <code>%USER_HOME%/zoodb</code> or <code>~/zoodb</code>. 
     * 
     * If a full path is given, the full path will be used.
     * 
     * Any necessary parent folders are created automatically.
     * 
     * It is recommended to use <code>.zdb</code> as file extension, for example 
     * <code>myDatabase.zdb</code>.
     * 
     * @param dbName Database name or full path.
     * @see DataStoreManager#createDb(String)
     */
    public ZooJdoProperties(String dbName) {
        super();
    	DBTracer.logCall(ZooJdoProperties.class, dbName); 
        String dbPath = ZooHelper.getDataStoreManager().getDbPath(dbName);
        setProperty(Constants.PROPERTY_PERSISTENCE_MANAGER_FACTORY_CLASS,
                PersistenceManagerFactoryImpl.class.getName());
        setProperty(Constants.PROPERTY_CONNECTION_URL, dbPath);
        setProperty(Constants.OPTION_OPTIMISTIC, Boolean.toString(true));
        setProperty(Constants.PROPERTY_DETACH_ALL_ON_COMMIT, Boolean.toString(false));
    }
    
    
	public ZooJdoProperties setUserName(String userName) {
    	DBTracer.logCall(this, userName); 
		put(Constants.PROPERTY_CONNECTION_USER_NAME, userName);
		return this;
	}
	
	public ZooJdoProperties setUserPass(String userName, String password) {
    	DBTracer.logCall(this, userName, "password"); 
		put(Constants.PROPERTY_CONNECTION_USER_NAME, userName);
		put(Constants.PROPERTY_CONNECTION_PASSWORD, password);
		return this;
	}
	
	
	public ZooJdoProperties setSessionName(String name) {
    	DBTracer.logCall(this, name); 
		put(Constants.PROPERTY_NAME, name);
		return this;
	}

	
	public ZooJdoProperties setOptimisticLocking(boolean flag) {
    	DBTracer.logCall(this, flag); 
		put(Constants.PROPERTY_OPTIMISTIC, Boolean.toString(flag));
		return this;
	}


	/**
	 * Whether queries should ignore objects in the cache. Default is 'false'.
	 * @param flag
	 * @return this
	 * @see Constants#PROPERTY_IGNORE_CACHE
	 */
	public ZooJdoProperties setIgnoreCache(boolean flag) {
    	DBTracer.logCall(this, flag); 
		put(Constants.PROPERTY_IGNORE_CACHE, Boolean.toString(flag));
		return this;
	}
	
	
	/**
	 * Whether values should be retained after commit(). By default objects are evicted.
	 * @param flag
	 * @return this
	 * @see Constants#PROPERTY_RETAIN_VALUES
	 */
	public ZooJdoProperties setRetainValues(boolean flag) {
    	DBTracer.logCall(this, flag); 
		put(Constants.PROPERTY_RETAIN_VALUES, Boolean.toString(flag));
		return this;
	}


	/**
	 * Whether objects should be detached during commit(). By default objects are not detached.
	 * @param flag
	 * @return this
	 * @see Constants#PROPERTY_DETACH_ALL_ON_COMMIT
	 */
	public ZooJdoProperties setDetachAllOnCommit(boolean flag) {
    	DBTracer.logCall(this, flag); 
		put(Constants.PROPERTY_DETACH_ALL_ON_COMMIT, Boolean.toString(flag));
		return this;
	}
	
	
//	/**
//	 * Property that defines whether ZooDB allows SCOs (embedded object) other than the ones
//	 * required by the JDO standard, see {@link ZooConstants#PROPERTY_ALLOW_NON_STANDARD_SCOS}
//	 * for details. 
//	 * creation. Default is {@code false}.
//	 * @param flag
//	 * @return this
//	 * @see ZooConstants#PROPERTY_ALLOW_NON_STANDARD_SCOS
//	 */
//	public ZooJdoProperties setZooAllowNonStandardSCOs(boolean flag) {
//		put(ZooConstants.PROPERTY_ALLOW_NON_STANDARD_SCOS, Boolean.toString(flag));
//		return this;
//	}


	/**
	 * Property that defines whether schemata should be created as necessary or need explicit 
	 * creation. Default is {@code true}.
	 * Requiring explicit creation can for example be useful to prevent accidental schema changes.
	 * @param flag
	 * @return this
	 * @see ZooConstants#PROPERTY_AUTO_CREATE_SCHEMA
	 */
	public ZooJdoProperties setZooAutoCreateSchema(boolean flag) {
    	DBTracer.logCall(this, flag); 
		put(ZooConstants.PROPERTY_AUTO_CREATE_SCHEMA, Boolean.toString(flag));
		return this;
	}
	
	
	/**
	 * Property that defines whether evict() should also reset primitive values. By default, 
	 * ZooDB only resets references to objects, even though the JDO spec states that all fields
	 * should be evicted. 
	 * In a properly enhanced/activated class, the difference should no be noticeable, because
	 * access to primitive fields of evicted objects should always trigger a reload. Because of 
	 * this, ZooDB by default avoids the effort of resetting primitive fields.
	 * Default is {@code false}.
	 * @param flag
	 * @return this
	 * @see ZooConstants#PROPERTY_EVICT_PRIMITIVES
	 */
	public ZooJdoProperties setZooEvictPrimitives(boolean flag) {
    	DBTracer.logCall(this, flag); 
		put(ZooConstants.PROPERTY_EVICT_PRIMITIVES, Boolean.toString(flag));
		return this;
	}


	/**
	 * Property that defines whether PersistenceManagers should expect multi-threaded access. 
	 * Default is {@code true}.
	 * @param flag
	 * @return this
	 * @see Constants#PROPERTY_MULTITHREADED
	 */
	public ZooJdoProperties setMultiThreaded(boolean flag) {
		put(Constants.PROPERTY_MULTITHREADED, Boolean.toString(flag));
		return this;
	}

}
