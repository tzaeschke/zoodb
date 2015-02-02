/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

import java.util.Collection;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.util.Util;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooSchema;
import org.zoodb.tools.internal.ZooCommandLineTool;

/**
 * This tool allows performing queries on the command line.
 */
public class ZooQuery extends ZooCommandLineTool {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		String dbName;
		if (args.length < 1) {
			out.println("Usage:");
			out.println("ZooQuery <dbname> select from <class> where ... ");
			//System.out.println("  - <class>: package name can be omitted");
			System.exit(0);
		} 
		
		dbName = args[0];
		if (!ZooHelper.getDataStoreManager().dbExists(dbName)) {
			err.println("ERROR Database not found: " + dbName);
			return;
		}
		
		String className = args[3];
		
		out.println("Querying database: " + dbName);

		ZooJdoProperties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();
		
		ZooSchema s = ZooJdoHelper.schema(pm);
		ZooClass zooClass = null;
		for (ZooClass cls: s.getAllClasses()) {
			String cName = cls.getName();
			if (cName.equals(className)) {
				zooClass = cls;
				break;
			}
			if (cName.endsWith(className) && 
					cName.charAt(cName.length()-className.length()-1) == '.') {
				zooClass = cls;
				break;
			}
		}
		if (zooClass == null) {
			err.println("ERROR Class name not found: " + className);
			return;
		}

		className = zooClass.getName();
		args[3] = className;
		out.println("Class name: " + className);
		
		String query = "";
		for (int i = 1; i < args.length; i++) {
			query += args[i] + " ";
		}
		out.println("Query: " + query);

		Collection<ZooPC> c = (Collection<ZooPC>) pm.newQuery(query).execute();

		out.println();
		out.println("Results found");
		out.println("=============");

		for (ZooPC o: c) {
			out.println(Util.oidToString(pm.getObjectId(o)) + " -- " + o.toString());
		}

		out.println("=============");
		pm.currentTransaction().rollback();
		pm.close();
		pmf.close();
		out.println("Querying database done.");
	}

}
