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
package org.zoodb.tools.impl;

import org.zoodb.jdo.ZooJdoProperties;


/**
 * This interface provides functionality to perform database management operations, such as
 * creating and removing database.
 * 
 * By default databases are create in %USER_HOME%/zoodb on Windows or ~/zoodb on Linux/UNIX.
 * 
 * @author ztilmann
 */
public interface DataStoreManager {

	/**
	 * Create a database file.
	 * 
	 * If only a file name is given, the database will create in %USER_HOME%/zoodb on Windows 
	 * or ~/zoodb on Linux/UNIX.
	 * 
	 * If a full path is given, the full path will be used instead.
	 * 
     * Any necessary parent folders are created automatically.

     * It is recommended to use <code>.zdb</code> as file extension, for example 
     * <code>myDatabase.zdb</code>.
     * 
	 * @param dbName The database file name or path 
	 * @see ZooJdoProperties#ZooJdoProperties(String)
	 */
	void createDb(String dbName);
	
	/**
	 * Check if a database exists. This checks only whether the file exists, not whether it is a 
	 * valid database file.
	 * 
	 * @param dbName The database file name or path 
	 * @return <code>true</code> if the database exists.
	 */
	boolean dbExists(String dbName);

    /**
     * Delete a database(-file).
     * @param dbName The database file name or path 
     * @return {@code true} if the database could be removed, otherwise false
     */
	boolean removeDb(String dbName);
	
    /**
     * 
     * @return The default database folder.
     */
	String getDefaultDbFolder();
	
	/**
	 * Calculates the full path for the given database name, whether the database exists or not.
	 * @param dbName The database file name or path 
	 * @return The full path of the database given by <code>dbName</code>.
	 */
	String getDbPath(String dbName);
}
