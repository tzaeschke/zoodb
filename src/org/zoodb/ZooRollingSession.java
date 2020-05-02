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
package org.zoodb;

import java.util.Collection;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.tools.ZooHelper;

/**
 * The main ZooDB rolling session class. Rolling sessions don't need to call {@code begin()}
 * after {@code commit()} or {@code rollback()}. Instead there is always an open transaction.
 * Note that while this can be very convenient, it may affect server performance in concurrent
 * scenarios.
 * 
 * @author ztilmann
 * @deprecated Currently not supported, please use JDO instead
 */
public class ZooRollingSession {

	//TODO: The concurrency aspect could be improved by internally starting the session when the 
	//first object is accessed or when the first API method is called, such as newQuery(). 

	
	//private final Session tx;
	private final PersistenceManager pm;
	
	private ZooRollingSession(String dbName) {
		pm = ZooJdoHelper.openDB(dbName);
		pm.currentTransaction().begin();
	}
	
	/**
	 * Opens the database at the given location. 
	 * The database is created if it does not exist.
	 * By default databases are created in %USER_HOME%/zoodb. 
	 * 
	 * @param dbName database name
	 * @return ZooRollingSession object
	 */
	static ZooRollingSession open(String dbName) {
        if (!ZooHelper.dbExists(dbName)) {
            // create database
            // By default, all database files will be created in %USER_HOME%/zoodb
            ZooHelper.createDb(dbName);
        }
		return new ZooRollingSession(dbName);
	}
	
	public void commit() {
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
	}
	
	public void rollback() {
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
	}
	
	public void makePersistent(Object pc) {
		pm.makePersistent(pc);
	}
	
	public void delete(Object pc) {
		pm.deletePersistent(pc);
	}
	
	public void close() {
		if (pm.currentTransaction().isActive()) {
			pm.currentTransaction().rollback();
		}
		pm.close();
		pm.getPersistenceManagerFactory().close();
	}
	
	public Collection<?> query(String query) {
		return (Collection<?>) pm.newQuery(query).execute();
	}
}

