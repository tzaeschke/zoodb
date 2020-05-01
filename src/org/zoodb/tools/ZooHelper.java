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
package org.zoodb.tools;

import java.lang.reflect.Constructor;

import org.zoodb.internal.util.DBLogger;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.tools.impl.DataStoreManager;

public class ZooHelper {

	private static DataStoreManager INSTANCE = null;

	public static DataStoreManager getDataStoreManager() {
		if (INSTANCE != null && ZooConfig.getFileManager().equals(INSTANCE.getClass().getName())) {
			return INSTANCE;
		}
		
		//create a new one
		try {
			Class<?> cls = Class.forName(ZooConfig.getFileManager());
			Constructor<?> con = cls.getConstructor();
			DataStoreManager dsm = (DataStoreManager) con.newInstance();
			INSTANCE = dsm;
			return dsm;
		} catch (Exception e) {
			throw DBLogger.newFatal("", e);
		}
	}
	
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
	 * @param dbName The database name or path
	 * @see ZooJdoProperties#ZooJdoProperties(String)
	 * @see DataStoreManager#createDb(String)
	 */
	public static void createDb(String dbName) {
		getDataStoreManager().createDb(dbName);
	}
	
	/**
	 * Check if a database exists. This checks only whether the file exists, not whether it is a 
	 * valid database file.
	 * 
	 * @param dbName The database name or path
	 * @return <code>true</code> if the database exists.
	 * @see DataStoreManager#dbExists(String)
	 */
    public static boolean dbExists(String dbName) {
    	return getDataStoreManager().dbExists(dbName);
    }

    /**
     * Delete a database(-file).
     * @param dbName The database name or path
     * @return {@code true} if the database could be removed, otherwise false
	 * @see DataStoreManager#removeDb(String)
     */
    public static boolean removeDb(String dbName) {
    	return getDataStoreManager().removeDb(dbName);
    }
}
