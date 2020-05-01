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
