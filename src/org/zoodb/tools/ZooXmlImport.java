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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.schema.ZooClass;
import org.zoodb.tools.internal.DataDeSerializer;
import org.zoodb.tools.internal.ObjectCache;
import org.zoodb.tools.internal.ObjectCache.GOProxy;
import org.zoodb.tools.internal.SerializerTools;
import org.zoodb.tools.internal.XmlReader;

/**
 * Export a database to xml.
 * 
 * @author ztilmann
 *
 */
public class ZooXmlImport {

	private final Scanner scanner;
	
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
		public boolean needsFieldDeclarations = false;
		public ClsDef(String name, long oid, long superOid) {
			this.oid = oid;
			this.name = name;
			this.superOid = superOid;
		}
	}
	
	private static class FldDef {
//		int id;
		String name;
		String typeName;
		int arrayDim;
		public FldDef(int id, String name, String typeName, int arrayDim) {
//			this.id = id;
			this.name = name;
			this.typeName = typeName;
			this.arrayDim = arrayDim;
		}
	}
	
	private void readFromXML(PersistenceManager pm) {
		Session session = ((PersistenceManagerImpl)pm).getSession();
		ObjectCache cache = new ObjectCache(session);

		readlnM("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
		readln1("<database>");

		readln1("<schema>");
		HashMap<Long, ClsDef> classes = new HashMap<Long, ClsDef>(); 
		HashMap<String, ClsDef> classNames = new HashMap<String, ClsDef>();
		while (readln1("<class", "</schema>")) {
			String name = readValue1("name");
			String oidStr = readValue1("oid");
			long sOid = Long.parseLong(oidStr);
			long superOid = Long.parseLong(readValue1("super"));
			
			//define schema
			ClsDef cd = new ClsDef(name, sOid, superOid);
			classes.put(sOid, cd);
			classNames.put(name, cd);

//			//TODO remove
//			try {
//				Class<?> cls = Class.forName(name);
//				ZooClass schema = ZooSchema.locateClass(pm, cls);
//				if (schema == null) {
//					schema = ZooSchema.defineClass(pm, cls);
//				}
//				schemata.put(sOid, schema);
//				cache.addSchema(sOid, ((ZooClassProxy) schema).getSchemaDef());
//			} catch (ClassNotFoundException e) {
//				throw new RuntimeException(e);
//			}
//			//TODO ---
			
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
			//50/51 are ZooPCImpl and PersistenceCapableImpl
			while (!definedClasses.containsKey(cd.superOid) && cd.superOid != 50) {
				//declare super-class first
				cd = classes.get(cd.superOid);
			}
			//Some schemata are predefined ...
			ZooClass schema = ZooJdoHelper.schema(pm).getClass(cd.name);
			if (schema == null) {
				ZooClass scd = definedClasses.get(cd.superOid);
				schema = ZooJdoHelper.schema(pm).defineEmptyClass(cd.name, scd);
				cd.needsFieldDeclarations = true;
			}
			
			classes.remove(cd.oid);
			definedClasses.put(cd.oid, schema);
			cache.addSchema(cd.oid, ((ZooClassProxy)schema).getSchemaDef());
		}
		
		//add attributes
		for (ClsDef cd: classNames.values()) {
			if (!cd.needsFieldDeclarations) {
				continue;
			}
			ZooClass schema = cache.getSchema(cd.oid).getVersionProxy();
			for (FldDef f: cd.fields) {
				if (classNames.containsKey(f.name)) {
					ClsDef cdType = classNames.get(f.name);
					ZooClass type = cache.getSchema(cdType.oid).getVersionProxy();
					schema.addField(f.name, type, f.arrayDim);
					System.out.println("class found for: " + f.typeName + " : " + type.getName());
				} else {
					Class<?> cls = createArrayClass(f.arrayDim, f.typeName);
					schema.addField(f.name, cls);
				}
			}
		}
		
		XmlReader r = new XmlReader(scanner);
		DataDeSerializer ser = new DataDeSerializer(r, cache);
		readln1("<data>");
		while (readln1("<class", "</data>")) {
			long sOid = Long.parseLong(readValue1("oid"));
			readValue1("name");
			ZooClass cls = definedClasses.get(sOid);

			
			while (readln1("<object", "</class>")) {
				String oidStr = readValue1("oid");
				long oid = Long.parseLong(oidStr);

				GOProxy hdl = cache.findOrCreateGo(oid, cls);
				
				ser.readGenericObject(oid, sOid, hdl);
				readln1("</object>");
			}
			//readln("</class>");
		}
		//readln("</data>");

		readln1("</database>");
	}

	private static Class<?> createArrayClass(int dims, String innerType) {
		try {
//			char[] ca = new char[dims];
//			Arrays.fill(ca, '[');
//			Class<?> compClass =  Class.forName(String.valueOf(ca) + innerType);
			Class<?> compClass =  Class.forName(innerType);
			return compClass;
		} catch (ClassNotFoundException e) {
			//throw new RuntimeException(e);
			//uhh, exceptions in normal code-flow, nice :-)
		}
		Class<?> c = SerializerTools.getPrimitiveType(innerType);
		if (c == null) {
			throw new IllegalArgumentException("Type not found: " + innerType);
		}
//		if (dims > 0) {
//			int[] dimDummy = new int[dims];
//			Arrays.fill(dimDummy, 1);
//			c = Array.newInstance(c, dimDummy).getClass();
//		}
		return c;
	}
	
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