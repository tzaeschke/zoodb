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
	 * @param dbName
	 * @see ZooJdoProperties#ZooJdoProperties(String)
	 */
	public void createDb(String dbName);
	
	/**
	 * Check if a database exists. This checks only whether the file exists, not whether it is a 
	 * valid database file.
	 * 
	 * @param dbName
	 * @return <code>true</code> if the database exists.
	 */
    public boolean dbExists(String dbName);

    /**
     * Delete a database(-file).
     * @param dbName
     * @return {@code true} if the database could be removed, otherwise false
     */
    public boolean removeDb(String dbName);
	
    /**
     * 
     * @return The default database folder.
     */
	public String getDefaultDbFolder();
	
	/**
	 * Calculates the full path for the given database name, whether the database exists or not.
	 * @param dbName
	 * @return The full path of the database given by <code>dbName</code>.
	 */
	public String getDbPath(String dbName);
}
