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
package org.zoodb;

import java.util.Collection;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.tools.ZooHelper;

/**
 * The main ZooDB session class.
 * 
 * @author ztilmann
 * @deprecated Currently not supported, please use JDO instead
 */
public class ZooSession {

	//private final Session tx;
	private final PersistenceManager pm;
	
	private ZooSession(String dbName) {
		pm = ZooJdoHelper.openDB(dbName);
	}
	
	/**
	 * Opens the database at the given location. 
	 * The database is created if it does not exist.
	 * By default databases are created in %USER_HOME%/zoodb. 
	 * 
	 * @param dbName
	 * @return ZooRollingSession object
	 */
	static final ZooSession open(String dbName) {
        if (!ZooHelper.dbExists(dbName)) {
            // create database
            // By default, all database files will be created in %USER_HOME%/zoodb
            ZooHelper.createDb(dbName);
        }
		return new ZooSession(dbName);
	}
	
	public void begin() {
		pm.currentTransaction().begin();
	}
	
	public void commit() {
		pm.currentTransaction().commit();
	}
	
	public void rollback() {
		pm.currentTransaction().rollback();
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

