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
package org.zoodb.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.internal.util.Util;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooField;
import org.zoodb.schema.ZooHandle;

public class ZooCompareDb {

	private static boolean logToConsole = false; 
	private static StringBuilder out;

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
		PersistenceManager pm1 = null; 
		PersistenceManager pm2 = null; 
		try {
			out = new StringBuilder(); //do this only here for repeated calls from tests
			ZooJdoProperties props1 = new ZooJdoProperties(db1);
			PersistenceManagerFactory pmf1 = JDOHelper.getPersistenceManagerFactory(props1);
			pm1 = pmf1.getPersistenceManager();
			pm1.currentTransaction().begin();
	
			ZooJdoProperties props2 = new ZooJdoProperties(db2);
			PersistenceManagerFactory pmf2 = JDOHelper.getPersistenceManagerFactory(props2);
			pm2 = pmf2.getPersistenceManager();
			pm2.currentTransaction().begin();
	
			List<ZooClass> commonClasses = compareClasses(pm1, pm2);
			compareInstances(pm1, pm2, commonClasses);
			return out.toString();
		} finally {
			if (pm1 != null) {
				pm1.currentTransaction().rollback();
				pm1.close();
			}
			if (pm2 != null) {
				pm2.currentTransaction().rollback();
				pm2.close();
			}
		}
	}
	
	private static List<ZooClass> compareClasses(PersistenceManager pm1, PersistenceManager pm2) {
		//List of classes that are identical in both databases
		List<ZooClass> commonClasses = new ArrayList<ZooClass>();
		for (ZooClass cls1: ZooJdoHelper.schema(pm1).getAllClasses()) {
			boolean isValid = true;
			if (cls1.getName().contains("ZooClass")) {
				isValid = false;
				continue;
			}
			ZooClass cls2 = ZooJdoHelper.schema(pm2).getClass(cls1.getName());
			if (cls2 == null) {
				log("Class not found in db2: " + cls1);
				isValid = false;
				continue;
			}
			for (ZooField f1: cls1.getAllFields()) {
				ZooField f2 = cls2.getField(f1.getName());
				if (f2 == null) {
					log("Class field not found in db2: " + cls1.getName() + "." + f1.getName());
					isValid = false;
					continue;
				}
				if (!f1.getTypeName().equals(f2.getTypeName())) {
					log("Class field type differ for: " + cls1.getName() + "." + f1.getName() + 
							"  " + f1.getTypeName() + " vs " + f2.getTypeName());
					isValid = false;
					continue;
				}
			}
			for (ZooField f2: cls2.getAllFields()) {
				ZooField f1 = cls1.getField(f2.getName());
				if (f1 == null) {
					log("Class field not found in db1: " + cls1.getName() + "." + f2.getName());
					isValid = false;
					continue;
				}
			}
			if (isValid) {
				commonClasses.add(cls1);
			}
		}
		for (ZooClass cls2: ZooJdoHelper.schema(pm2).getAllClasses()) {
			if (cls2.getName().contains("ZooClass")) {
				continue;
			}
			ZooClass cls1 = ZooJdoHelper.schema(pm1).getClass(cls2.getName());
			if (cls1 == null) {
				log("Class not found in db1: " + cls2);
				continue;
			}
		}
		return commonClasses;
	}

	private static void compareInstances(PersistenceManager pm1,
			PersistenceManager pm2, List<ZooClass> commonClasses) {
		for (ZooClass cls1: commonClasses) {
			ZooClass cls2 = ZooJdoHelper.schema(pm2).getClass(cls1.getName());
			Iterator<ZooHandle> i1 = cls1.getHandleIterator(false);
			while (i1.hasNext()) {
				ZooHandle hdl1 = i1.next();
				ZooHandle hdl2 = ZooJdoHelper.schema(pm2).getHandle(hdl1.getOid());
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
				for (ZooField f1: cls1.getAllFields()) {
					ZooField f2 = cls2.getField(f1.getName());
					Object v1 = f1.getValue(hdl1);
					Object v2 = f2.getValue(hdl2);
					if (v1 == v2) {
						//e.g. null
						continue;
					}
					if (v1 == null || v2 == null) {
						log("Field has different values: " + Util.oidToString(hdl1.getOid()) + 
								" " + cls1 + "." + f1.getName() + ": " + 
								f1.getValue(hdl1) + " vs " + f2.getValue(hdl2));
						continue;
					}
					if (v1.getClass() != v2.getClass()) {
						//For arrays this also covers dimensions and inner type
						log("Field value has different type: " + Util.oidToString(hdl1.getOid()) + 
								" " + cls1 + "." + f1.getName() + ": " + 
								v1.getClass() + " vs " + v1.getClass());
						continue;
					}
					if (v1.getClass().isArray() && arrayEquals(v1, v2)) {
						continue;
					}
					if (!v1.equals(v2)) {
						log("Field has different values: " + Util.oidToString(hdl1.getOid()) + 
								" " + cls1 + "." + f1.getName() + ": " + 
								f1.getValue(hdl1) + " vs " + f2.getValue(hdl2));
						continue;
					}
				}
			}
		}
		//reverse check
		for (ZooClass cls1: commonClasses) {
			ZooClass cls2 = ZooJdoHelper.schema(pm2).getClass(cls1.getName());
			Iterator<ZooHandle> i2 = cls2.getHandleIterator(false);
			while (i2.hasNext()) {
				ZooHandle hdl2 = i2.next();
				ZooHandle hdl1 = ZooJdoHelper.schema(pm1).getHandle(hdl2.getOid());
				if (hdl1 == null) {
					log("Object not found in db1: " + Util.oidToString(hdl2.getOid()) + " " + cls2);
					continue;
				}
			}		
		}
	}

	private static boolean arrayEquals(Object v1, Object v2) {
		if (v1.getClass().getComponentType() == Boolean.TYPE) {
			return Arrays.equals((boolean[])v1, (boolean[])v2);
		}
		if (v1.getClass().getComponentType() == Byte.TYPE) {
			return Arrays.equals((byte[])v1, (byte[])v2);
		}
		if (v1.getClass().getComponentType() == Character.TYPE) {
			return Arrays.equals((char[])v1, (char[])v2);
		}
		if (v1.getClass().getComponentType() == Double.TYPE) {
			return Arrays.equals((double[])v1, (double[])v2);
		}
		if (v1.getClass().getComponentType() == Float.TYPE) {
			return Arrays.equals((float[])v1, (float[])v2);
		}
		if (v1.getClass().getComponentType() == Integer.TYPE) {
			return Arrays.equals((int[])v1, (int[])v2);
		}
		if (v1.getClass().getComponentType() == Long.TYPE) {
			return Arrays.equals((long[])v1, (long[])v2);
		}
		if (v1.getClass().getComponentType() == Short.TYPE) {
			return Arrays.equals((short[])v1, (short[])v2);
		}
		return Arrays.deepEquals((Object[])v1, (Object[])v2);
	}

	private static void log(String s) {
		if (logToConsole) {
			System.out.println(s);
		} else {
			out.append(s + '\n');
		}
	}
}
