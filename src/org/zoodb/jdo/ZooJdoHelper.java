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
package org.zoodb.jdo;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.internal.Session;
import org.zoodb.internal.util.DBTracer;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooSchema;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.ZooHelper;

public class ZooJdoHelper extends ZooHelper {

	/**
     * Open a database.
     * 
     * This is short for: <br> 
     * <code>
     * ZooJdoProperties props = new ZooJdoProperties(dbName);  <br>
     * PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);  <br>
     * PersistenceManager pm = pmf.getPersistenceManager();  <br>
     * </code>
     * 
     * @param dbName Name of the database to connect to.
     * @return A new PersistenceManager
     */
    public static PersistenceManager openDB(String dbName) {
    	DBTracer.logCall(ZooJdoHelper.class, dbName); 
    	ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		return pmf.getPersistenceManager();
    }

    
	/**
     * Open a database.
     * Create the database file if it doesn't exist.
     * 
     * This is short for: <br> 
     * <code>
     * ZooJdoProperties props = new ZooJdoProperties(dbName);  <br>
     * PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);  <br>
     * PersistenceManager pm = pmf.getPersistenceManager();  <br>
     * if (!dbExists(dbName)) { <br>
     *   createDb(dbName); <br>
     * } <br>
     * </code>
     * 
     * @param dbName Name of the database to connect to.
     * @return A new PersistenceManager
     */
    public static PersistenceManager openOrCreateDB(String dbName) { 
    	DBTracer.logCall(ZooJdoHelper.class, dbName); 
    	if (!dbExists(dbName)) {
    		createDb(dbName);
    	}
    	return openDB(dbName);
    }
    
    /**
     * Get access to ZooDB schema management methods.
     * 
     * @param pm The PersistenceManager
     * @return the schema management API
     */
    public static ZooSchema schema(PersistenceManager pm) {
    	DBTracer.logCall(ZooJdoHelper.class, pm); 
    	return ((PersistenceManagerImpl)pm).getSession().schema();
    }
    
    /**
     * A convenience method for creating indices.
	 * Creates an index on the specified field for the current class and all sub-classes.
	 * The method will create a schema for the class if none exists.
     * @param pm The PersistenceManager
     * @param cls The class
     * @param fieldName The field name
     * @param isUnique Whether the index should be only allow unique keys
     */
    public static void createIndex(PersistenceManager pm, Class<?> cls, String fieldName, 
    		boolean isUnique) {
    	DBTracer.logCall(ZooJdoHelper.class, pm, cls, fieldName, isUnique); 
    	ZooSchema s = schema(pm);
    	ZooClass c = s.getClass(cls); 
    	if (c == null) {
    		c = s.addClass(cls);
    	}
    	c.createIndex(fieldName, isUnique);
    }

    /**
     * Get access to the statistics API of ZooDB.
     * @param pm The PersistenceManager
     * @return the statistics manager
     */
	public static DBStatistics getStatistics(PersistenceManager pm) {
    	DBTracer.logCall(ZooJdoHelper.class, pm); 
		Session s = (Session) pm.getDataStoreConnection().getNativeConnection();
		return new DBStatistics(s);
	}
}
