/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.api.ZooHandle;
import org.zoodb.jdo.api.ZooHelper;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.internal.util.Util;

public class ZooCompareDb {

	private static boolean logToConsole = false; 
	private static StringBuilder out = new StringBuilder();

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Error: Please provide two databases.");
            System.out.println("Usage: ");
            System.out.println("    ZooCompareDb <db1> <db2>");
		}
		String db1 = args[0];
		String db2 = args[1];
		if (!ZooHelper.getDataStoreManager().dbExists(db1)) {
			System.err.println("ERROR Database not found: " + db1);
			return;
		}
		
		if (!ZooHelper.getDataStoreManager().dbExists(db2)) {
			System.err.println("ERROR Database not found: " + db2);
			return;
		}
		
		System.out.println("Checking databases: " + db1 + " <--> " + db2);
		run(db1, db2);
		System.out.println("Comparing databases finished.");
	}
	
	public static String run(String db1, String db2) {
		ZooJdoProperties props1 = new ZooJdoProperties(db1);
		PersistenceManagerFactory pmf1 = JDOHelper.getPersistenceManagerFactory(props1);
		PersistenceManager pm1 = pmf1.getPersistenceManager();
		pm1.currentTransaction().begin();

		ZooJdoProperties props2 = new ZooJdoProperties(db2);
		PersistenceManagerFactory pmf2 = JDOHelper.getPersistenceManagerFactory(props2);
		PersistenceManager pm2 = pmf2.getPersistenceManager();
		pm2.currentTransaction().begin();

		List<ZooClass> commonClasses = compareClasses(pm1, pm2);
		compareInstances(pm1, pm2, commonClasses);

		pm1.currentTransaction().rollback();
		pm1.close();
		pmf1.close();
		pm2.currentTransaction().rollback();
		pm2.close();
		pmf2.close();
		return out.toString();
	}
	
	private static List<ZooClass> compareClasses(PersistenceManager pm1, PersistenceManager pm2) {
		//List of classes that are identical in both databases
		List<ZooClass> commonClasses = new ArrayList<ZooClass>();
		for (ZooClass cls1: ZooSchema.locateAllClasses(pm1)) {
			if (cls1.getName().contains("ZooClass")) {
				continue;
			}
			ZooClass cls2 = ZooSchema.locateClass(pm2, cls1.getName());
			if (cls2 == null) {
				log("Class not found in db2: " + cls1);
				continue;
			}
			for (ZooField f1: cls1.getAllFields()) {
				ZooField f2 = cls2.locateField(f1.getName());
				if (f2 == null) {
					log("Class field not found in db2: " + cls1.getName() + "." + f1.getName());
					continue;
				}
				if (!f1.getTypeName().equals(f2.getTypeName())) {
					log("Class field type differ for: " + cls1.getName() + "." + f1.getName() + 
							"  " + f1.getTypeName() + " vs " + f2.getTypeName());
					continue;
				}
			}
			for (ZooField f2: cls2.getAllFields()) {
				ZooField f1 = cls1.locateField(f2.getName());
				if (f1 == null) {
					log("Class field not found in db1: " + cls1.getName() + "." + f2.getName());
					continue;
				}
			}
			commonClasses.add(cls1);
		}
		for (ZooClass cls2: ZooSchema.locateAllClasses(pm2)) {
			if (cls2.getName().contains("ZooClass")) {
				continue;
			}
			ZooClass cls1 = ZooSchema.locateClass(pm1, cls2.getName());
			if (cls1 == null) {
				log("Class not found in db1: " + cls1);
				continue;
			}
		}
		return commonClasses;
	}

	private static void compareInstances(PersistenceManager pm1,
			PersistenceManager pm2, List<ZooClass> commonClasses) {
		for (ZooClass cls1: commonClasses) {
			ZooClass cls2 = ZooSchema.locateClass(pm2, cls1.getName());
			Iterator<ZooHandle> i1 = cls1.getHandleIterator(false);
			while (i1.hasNext()) {
				ZooHandle hdl1 = i1.next();
				ZooHandle hdl2 = ZooSchema.locateObject(pm2, hdl1.getOid());
				if (hdl2 == null) {
					log("Object not found in db2: " + Util.oidToString(hdl1.getOid()) + " " + cls1);
					continue;
				}
				//compare object type
				if (!hdl1.getType().getName().equals(hdl2.getType().getName())) {
					log("Object has different classes: " + Util.oidToString(hdl1.getOid()) + 
							" " + cls1 + " vs " + hdl2.getType());
					continue;
				}
				//compare object data
				for (ZooField f: cls1.getAllFields()) {
					//TODO compare object
				}
			}
		}
		//TODO reverse-check for objects
		
	}

	private static void log(String s) {
		if (logToConsole) {
			System.out.println(s);
		} else {
			out.append(s + '\n');
		}
	}
}
