package org.zoodb.jdo.api;

import java.util.Properties;

import javax.jdo.Constants;

import org.zoodb.jdo.PersistenceManagerFactoryImpl;
import org.zoodb.jdo.ZooConstants;

/**
 * Properties to be used for creating JDO session.
 * <p>
 * <code>
 * ZooJdoProperties props = new ZooJdoProperties("MyDatabase");  <br> 
 * JDOHelper.getPersistenceManagerFactory(props);    <br>
 * </code>
 * 
 * 
 * @author Tilmann Zaeschke
 */
public class ZooJdoProperties extends Properties implements Constants {

	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new set of properties for creating a new persistence manager
	 * with 
	 * @param dbName
	 */
	public ZooJdoProperties(String dbName) {
		super();
		setProperty(Constants.PROPERTY_PERSISTENCE_MANAGER_FACTORY_CLASS,
				PersistenceManagerFactoryImpl.class.getName());
		setProperty(Constants.PROPERTY_CONNECTION_URL, dbName);
	}
	
	
	public ZooJdoProperties setUserName(String userName) {
		put(Constants.PROPERTY_CONNECTION_USER_NAME, userName);
		return this;
	}
	
	public ZooJdoProperties setUserPass(String userName, String password) {
		put(Constants.PROPERTY_CONNECTION_USER_NAME, userName);
		put(Constants.PROPERTY_CONNECTION_PASSWORD, password);
		return this;
	}
	
	
	public ZooJdoProperties setSessionName(String name) {
		put(Constants.PROPERTY_NAME, name);
		return this;
	}

	
	public ZooJdoProperties setOptimisticLocking(boolean flag) {
		put(Constants.PROPERTY_OPTIMISTIC, Boolean.toString(flag));
		return this;
	}


	public ZooJdoProperties setAutoCreateSchema(boolean flag) {
		put(ZooConstants.PROPERTY_AUTO_CREATE_SCHEMA, Boolean.toString(flag));
		return this;
	}
	
	
	/**
	 * Whether queries should ignore objects in the cache. Default is 'false'.
	 * @param flag
	 * @return this.
	 * @see Constants#PROPERTY_IGNORE_CACHE
	 */
	public ZooJdoProperties setIgnoreCache(boolean flag) {
		put(Constants.PROPERTY_IGNORE_CACHE, Boolean.toString(flag));
		return this;
	}
}
