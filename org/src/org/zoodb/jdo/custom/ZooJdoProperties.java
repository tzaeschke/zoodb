package org.zoodb.jdo.custom;

import java.util.Properties;

import javax.jdo.Constants;

import org.zoodb.jdo.PersistenceManagerFactoryImpl;

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
public class ZooJdoProperties extends Properties {

	
	
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
		put(Constants.PROPERTY_OPTIMISTIC, flag);
		return this;
	}
}
