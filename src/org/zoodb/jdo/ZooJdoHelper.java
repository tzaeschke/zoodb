/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.internal.Session;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooSchema;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.ZooHelper;

public class ZooJdoHelper extends ZooHelper {

	/**
     * Open a new database connection.
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
        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        return pm;
    }

    
    /**
     * Get access to ZooDB schema management methods.
     * 
     * @param pm
     * @return the schema management API
     */
    public static ZooSchema schema(PersistenceManager pm) {
    	return ((PersistenceManagerImpl)pm).getSession().schema();
    }
    
    /**
     * A convenience method for creating indices.
     * @param pm
     * @param cls
     * @param fieldName
     * @param isUnique Whether the index should be only allow unique keys
     */
    public static void createIndex(PersistenceManager pm, Class<?> cls, String fieldName, 
    		boolean isUnique) {
    	ZooSchema s = schema(pm);
    	ZooClass c = s.getClass(cls); 
    	if (c == null) {
    		c = s.addClass(cls);
    	}
    	c.createIndex(fieldName, isUnique);
    }

    /**
     * Get access to the statistics API of ZooDB.
     * @param pm
     * @return the statistics manager
     */
	public static DBStatistics getStatistics(PersistenceManager pm) {
		Session s = (Session) pm.getDataStoreConnection().getNativeConnection();
		return new DBStatistics(s);
	}
}
