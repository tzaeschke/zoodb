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

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.Session;
import org.zoodb.internal.SessionConfig;
import org.zoodb.tools.ZooHelper;

/**
 * The main ZooDB session class.
 * 
 * @author ztilmann
 * @deprecated Currently not supported, please use JDO instead
 */
public class ZooSession {

	private final Session tx;
	
	private ZooSession(String dbName) {
		SessionConfig cfg = new SessionConfig();
		cfg.setAutoCreateSchema(true);
		tx = new Session(dbName, cfg);
	}
	
	/**
	 * Opens the database at the given location. 
	 * The database is created if it does not exist.
	 * By default databases are created in %USER_HOME%/zoodb. 
	 * 
	 * @param dbName The database name or path
	 * @return ZooRollingSession object
	 */
	public static final ZooSession open(String dbName) {
        if (!ZooHelper.dbExists(dbName)) {
            // create database
            // By default, all database files will be created in %USER_HOME%/zoodb
            ZooHelper.createDb(dbName);
        }
		return new ZooSession(dbName);
	}
	
	public void begin() {
		tx.begin();
	}
	
	public void commit() {
		tx.commit(false);
	}
	
	public void rollback() {
		tx.rollback();
	}
	
	public void makePersistent(Object pc) {
		//TODO casting is not ideal, better check type?
		tx.makePersistent((ZooPC) pc);
	}
	
	public void delete(Object pc) {
		tx.deletePersistent(pc);
	}
	
	public void close() {
		if (tx.isActive()) {
			tx.rollback();
		}
		tx.close();
	}
	
	public Collection<?> query(String query) {
		throw new UnsupportedOperationException();

		//return (Collection<?>) tx.newQuery(query).execute();
	}
}

