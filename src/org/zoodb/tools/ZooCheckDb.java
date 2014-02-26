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
package org.zoodb.tools;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.internal.Session;
import org.zoodb.jdo.ZooJdoProperties;

public class ZooCheckDb {

	private static final String DB_NAME = "TestDb"; 
	//private static final String DB_NAME = "RandomRegularGraph-n1000-d20";
	//private static final String DB_NAME = "zoodb"; 
	//private static final String DB_NAME = "D:\\data\\SEDD-12-08\\stackoverflow2.zdb";
	//private static final String DB_NAME = "StackBicycles";
	//private static final String DB_NAME = "StackServerFault";

	public static void main(String[] args) {
		String dbName;
		if (args.length == 0) {
			dbName = DB_NAME;
		} else {
			dbName = args[0];
		}
		
		if (!ZooHelper.getDataStoreManager().dbExists(dbName)) {
			System.err.println("ERROR Database not found: " + dbName);
			return;
		}
		
		System.out.println("Checking database: " + dbName);

		ZooJdoProperties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		Session s = (Session) pm.getDataStoreConnection().getNativeConnection();
		String report = s.getPrimaryNode().checkDb();
		System.out.println();
		System.out.println("Report");
		System.out.println("======");

		System.out.println(report);

		System.out.println("======");
		pm.close();
		pmf.close();
		System.out.println("Checking database done.");
	}

}
