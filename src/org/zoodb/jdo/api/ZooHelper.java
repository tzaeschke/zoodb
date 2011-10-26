/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.api;

import java.lang.reflect.Constructor;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.PersistenceManager;

import org.zoodb.jdo.api.impl.DBStatistics;
import org.zoodb.jdo.internal.Config;
import org.zoodb.jdo.internal.Session;

public class ZooHelper {

	private static DataStoreManager INSTANCE = null;

	public static DataStoreManager getDataStoreManager() {
		if (INSTANCE != null && Config.getFileManager().equals(INSTANCE.getClass().getName())) {
			return INSTANCE;
		}
		
		//create a new one
		try {
			Class<?> cls = Class.forName(Config.getFileManager());
			Constructor<?> con = cls.getConstructor();
			DataStoreManager dsm = (DataStoreManager) con.newInstance();
			INSTANCE = dsm;
			return dsm;
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}
	
	public static DBStatistics getStatistics(PersistenceManager pm) {
		Session s = (Session) pm.getDataStoreConnection().getNativeConnection();
		return new DBStatistics(s);
	}
}
