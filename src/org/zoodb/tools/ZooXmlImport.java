/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.internal.ZooClassProxy;
import org.zoodb.tools.internal.DataDeSerializer;
import org.zoodb.tools.internal.ObjectCache;
import org.zoodb.tools.internal.ObjectCache.GOProxy;
import org.zoodb.tools.internal.XmlReader;

/**
 * Export a database to xml.
 * 
 * @author ztilmann
 *
 */
public class ZooXmlImport {

	private final Scanner scanner;
	
	private final Map<Long, ZooClass> schemata = new HashMap<Long, ZooClass>();

	public ZooXmlImport(Scanner sc) {
		this.scanner = sc;
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Error: invalid number of arguments.");
			System.out.println("Usage: ");
			System.out.println("    XmlExport <dbName> <xmlFileName>");
			return;
		}

		String dbName = args[0];
		String xmlName = args[1];
		Scanner sc = openFile(xmlName);
		if (sc == null) {
			return;
		}

		try {
			new ZooXmlImport(sc).readDB(dbName);
		} finally {
			sc.close();
		}

		sc.close();
	}

	public void readDB(String dbName) {
		ZooJdoProperties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		try {
			pm.currentTransaction().begin();
			readFromXML(pm);
		} catch (Throwable t) {
			t.printStackTrace();
			throw new RuntimeException(t);
		} finally {
			pm.currentTransaction().commit();
			pm.close();
			pmf.close();
		}
	}

	private static class ClsDef {
		long oid;
		String name;
		long superOid;
		ArrayList<FldDef> fields = new ArrayList<FldDef>();
		public ClsDef(String name, long oid, long superOid) {
			this.oid = oid;
			this.name = name;
			this.superOid = superOid;
		}
	}
	
	private static class FldDef {
		int id;
		String name;
		String typeName;
		int arrayDim;
		public FldDef(int id, String name, String typeName, int arrayDim) {
			this.id = id;
			this.name = name;
			this.typeName = typeName;
			this.arrayDim = arrayDim;
		}
	}
	
	private void readFromXML(PersistenceManager pm) {
		ObjectCache cache = new ObjectCache();

		readlnM("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
		readln1("<database>");

		readln1("<schema>");
		HashMap<Long, ClsDef> classes = new HashMap<Long, ClsDef>(); 
		while (readln1("<class", "</schema>")) {
			String name = readValue1("name");
			String oidStr = readValue1("oid");
			long sOid = Long.parseLong(oidStr);
			long superOid = Long.parseLong(readValue1("super"));
			
			//define schema
			ClsDef cd = new ClsDef(name, sOid, superOid);

			int prevId = -1;
			while (readln1("<attr", "</class>")) {
				int id = Integer.parseInt(readValue1("id"));
				String attrName = readValue1("name");
				String typeName = readValue1("type");
				int arrayDim = Integer.parseInt(readValue1("arrayDim"));
				readln1("/>");

				//verify correct order off fields
				prevId++;
				if (prevId != id) {
					throw new IllegalStateException("Illegal field ordering: " + id);
				}
				
				FldDef f = new FldDef(id, attrName, typeName, arrayDim);
				cd.fields.add(f);
				
            }
			//readln("</class>");
		}
		
		//insert schema in database
		HashMap<Long, ZooClass> definedClasses = new HashMap<Long, ZooClass>();
		while (!classes.isEmpty()) {
			Iterator<ClsDef> itCD = classes.values().iterator();  
			ClsDef cd = itCD.next();
			while (!classes.containsKey(cd.superOid)) {
				//declare super-class first
				cd = classes.get(cd.superOid);
			}
			ZooClass schema = ZooSchema.locateClass(pm, cd.name);
			if (schema != null) {
				//Some schemata are predefined ...
				classes.remove(cd.oid);
				definedClasses.put(cd.oid, schema);
				continue;
			}
			
			
//						Class<?> cls = Class.forName(name);
//						schema = ZooSchema.defineClass(pm, cls);
			ZooClass scd = definedClasses.get(cd.superOid);
			schema = ZooSchema.declareClass(pm, scd.getName());
			classes.remove(cd.oid);
			definedClasses.put(cd.oid, schema);

			cache.addSchema(cd.superOid, ((ZooClassProxy)schema).getSchemaDef());
			for (FldDef f: cd.fields) {
//				Class<?> cls = deserializeArray(f.arrayDim, innerType, innerTypeAcronym);
//				schema.declareField(f.name, cls, f.arrayDim);
			}
		}
		
		XmlReader r = new XmlReader(scanner);
		DataDeSerializer ser = new DataDeSerializer(r, cache);
		readln1("<data>");
		while (readln1("<class", "</data>")) {
			long sOid = Long.parseLong(readValue1("oid"));
			readValue1("name");
			ZooClass cls = schemata.get(sOid);

			
			while (readln1("<object", "</class>")) {
				String oidStr = readValue1("oid");
				long oid = Long.parseLong(oidStr);
				//System.out.println("RR: oid=" + oid);

				GOProxy hdl = cache.findOrCreateGo(oid, cls);
				
				ser.readGenericObject(oid, sOid, hdl);
				readln1("</object>");
			}
			//readln("</class>");
		}
		//readln("</data>");

		readln1("</database>");
	}

	
//	private static Class<?> deserializeArray(int dims, String innerType, String innerTypeAcronym) {
//
//		// read data
//		return deserializeArrayColumn(innerType, innerTypeAcronym, dims);
//	}
//
//	private static final Class<?> deserializeArrayColumn(Class<?> innerType, 
//			String innerAcronym, int dims) {
//
//		Object array = null;
//
//		if (dims > 1) {
//			//Create multi-dimensional array
//			try {
//				char[] ca = new char[dims-1];
//				Arrays.fill(ca, '[');
//				Class<?> compClass =  Class.forName(new String(ca) + innerAcronym);
//				array = Array.newInstance(compClass, l);
//			} catch (ClassNotFoundException e) {
//				throw new RuntimeException(e);
//			}
//			for (int i = 0; i < l; i++) {
//				Array.set(array, i,  deserializeArrayColumn(innerType, innerAcronym, dims-1) );
//			}
//			return array.getClass();
//		}
//
//		array = Array.newInstance(innerType, l);
//		return array.getClass();
//	}
	
	private static Scanner openFile(String xmlName) {
		File file = new File(xmlName);
		if (!file.exists()) {
			System.out.println("File not found: " + file);
			return null;
		}

		try {
			FileInputStream fis = new FileInputStream(file);
			return new Scanner(fis, "UTF-8");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		} 
	}

	/**
	 * Read a value, e.g. class="x.y" return "x.y" for read("class").
	 * @param name
	 * @return value.
	 */
	private String readValue1(String name) {
		String in = scanner.next();
		if (!in.startsWith(name)) {
			throw new IllegalStateException("Expected " + name + " but got " + in);
		}
		if (in.endsWith(">")) {
			return in.substring(name.length() + 2, in.length()-2);
		} else {
			return in.substring(name.length() + 2, in.length()-1);
		}
	}

//	private String readValueM(String name) {
//		
//		//TODO
//		//TODO read multi!
//		//TODO
//		//TODO
//		String in = scanner.next();
//		if (!in.startsWith(name)) {
//			throw new IllegalStateException("Expected " + name + " but got " + in);
//		}
//		if (in.endsWith(">")) {
//			return in.substring(name.length() + 2, in.length()-2);
//		} else {
//			return in.substring(name.length() + 2, in.length()-1);
//		}
//	}

	private void readln1(String str) {
		String s2 = scanner.next();
		if (!s2.equals(str)) {
			throw new IllegalStateException("Expected: " + str + " but got: " + s2);
		}
	}

	private void readlnM(String str) {
		Scanner scStr = new Scanner(str);
		while (scStr.hasNext()) {
			String s1 = scStr.next();
			String s2 = scanner.next();
			if (!s2.equals(s1)) {
				scStr.close();
				throw new IllegalStateException("Expected: " + str + " but got: " + s2);
			}
		}
		scStr.close();
	}

	/**
	 * Reads '1' token.
	 * @param strExpected
	 * @param strAlternative
	 * @return true if 1st string matches, or false if second matches
	 * @throws IllegalStateException if neither String matches
	 */
	private boolean readln1(String strExpected, String strAlternative) {
		String sX = scanner.next();
		if (sX.equals(strExpected)) {
			return true;
		}
		if (sX.equals(strAlternative)) {
			return false;
		}
		throw new IllegalStateException("Expected: " + strAlternative + " but got: " + sX);
	}

	/**
	 * Reads multiple tokens.
	 * @param strExpected
	 * @param strAlternative
	 * @return true if 1st string matches, or false if second matches
	 * @throws IllegalStateException if neither String matches
	 */
//	private boolean readlnM(String strExpected, String strAlternative) {
//		Scanner scStr1 = new Scanner(strExpected);
//		Scanner scStr2 = new Scanner(strAlternative);
//		while (scStr1.hasNext()) {
//			String s1 = scStr1.next();
//			String s2 = null;
//			if (scStr2.hasNext()) {
//				s2 = scStr2.next();
//			}
//			String sX = scanner.next();
//			if (!sX.equals(s1)) {
//				if (sX.equals(s2)) {
//					while (scStr2.hasNext()) {
//						s2 = scStr2.next();
//						sX = scanner.next();
//						if (!sX.equals(s2)) {
//							scStr1.close();
//							scStr2.close();
//							throw new IllegalStateException(
//									"Expected: " + strAlternative + " but got: " + sX);
//						}
//					}
//					scStr1.close();
//					scStr2.close();
//					return false;
//				} else {
//					scStr1.close();
//					scStr2.close();
//					throw new IllegalStateException("Expected: " + strExpected + " but got: " + sX);
//				}
//			}
//		}
//		scStr1.close();
//		scStr2.close();
//		return true;
//	}
}