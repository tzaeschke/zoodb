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
package org.zoodb.test.testutil;

import java.lang.reflect.Field;
import java.util.Properties;

import javax.jdo.JDOException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.api.ZooDebug;
import org.zoodb.jdo.PersistenceManagerImpl;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

public class TestTools {

	private static final String DB_NAME = "TestDb";
	private static PersistenceManager pm;

	public static void createDb(String dbName) {
		if (ZooHelper.dbExists(dbName)) {
			removeDb(dbName);
		}
		ZooDebug.setTesting(true);
		ZooHelper.createDb(dbName);
	}
	

	/**
	 * Create the default database.
	 */
	public static void createDb() {
		createDb(DB_NAME);
	}
	
	
	/**
	 * Remove the default database.
	 */
	public static void removeDb() {
		removeDb(DB_NAME);
	}
	
	
	public static void removeDb(String dbName) {
		if (!ZooHelper.dbExists(dbName)) {
			return;
		}
		
		try {
			closePM();
		} catch (JDOException e) {
			e.printStackTrace();
		}
        //TODO implement loop ala http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038 ?
        //-> Entry from 23-MARCH-2005 #2
//      for (int i = 0; i < 100; i++) {
//          System.gc();
//          System.runFinalization();
//          //Thread.sleep(100);
//      }
		try {
			ZooHelper.removeDb(dbName);
		} catch (JDOUserException e) {
			//ignore
		}
	}

	/**
	 * Varargs does not seem to work with generics.
	 * @param classes
	 */
    public static void defineSchema(Class<?> ... classes) {
        defineSchema(DB_NAME, classes);
    }
	
    
	public static void defineSchema(String dbName, Class<?> ... classes) {
		Properties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = null;
		try {
		    pm = pmf.getPersistenceManager();

	        pm.currentTransaction().begin();
	        
	        for (Class<?> cls: classes) {
	        	ZooSchema.defineClass(pm, cls);
	        }

	        pm.currentTransaction().commit();
		} finally {
			safeClose(pmf, pm);
		}
	}

	public static void removeSchema(Class<?> ... classes) {
		Properties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = null;
        try {
            pm = pmf.getPersistenceManager();
	        pm.currentTransaction().begin();
            for (Class<?> cls: classes) {
            	ZooSchema.locateClass(pm, cls).remove();
            }
            pm.currentTransaction().commit();
        } finally {
			safeClose(pmf, pm);
        }
	}


	public static PersistenceManager openPM() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		return openPM(props);
	}


	public static PersistenceManager openPM(ZooJdoProperties props) {
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();
		return pm;
	}

	
	public static PersistenceManager openPM(String databaseName) {
		ZooJdoProperties props = new ZooJdoProperties(databaseName);
		return openPM(props);
	}

	
	public static void closePM(PersistenceManager pm) {
		PersistenceManagerFactory pmf = pm.getPersistenceManagerFactory();
		if (!pm.isClosed()) {
			try {
				if (pm.currentTransaction().isActive()) {
					pm.currentTransaction().rollback();
				}
				pm.close();
			} catch (Exception e) {
				e.printStackTrace();
				((PersistenceManagerImpl)pm).getSession().close();
			}
		}
		pmf.close();
		TestTools.pm = null;
		//close files that may still be open
		if (ZooDebug.isTesting()) {
			ZooDebug.closeOpenFiles();
			//ZooDebug.setTesting(false);
		}
	}


	public static void closePM() {
		if (pm != null) {
			closePM(pm);
		} else {
			//close files that may still be open
			if (ZooDebug.isTesting()) {
				ZooDebug.closeOpenFiles();
				//ZooDebug.setTesting(false);
			}
		}
	}


    public static String getDbFileName() {
        return ZooHelper.getDataStoreManager().getDbPath(DB_NAME);
    }


    public static String getDbName() {
        return DB_NAME;
    }


	public static void dropInstances(Class<?> ... classes) {
		Properties props = new ZooJdoProperties(DB_NAME);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = null;
		try {
		    pm = pmf.getPersistenceManager();

	        pm.currentTransaction().begin();
	        
	        for (Class<?> cls: classes) {
	        	ZooSchema.locateClass(pm, cls).dropInstances();
	        }

	        pm.currentTransaction().commit();
		} finally {
			safeClose(pmf, pm);
		}
	}
	
	private static void safeClose(PersistenceManagerFactory pmf, PersistenceManager pm) {
	    if (pm != null) {
	    	if (pm.currentTransaction().isActive()) {
	    		pm.currentTransaction().rollback();
	    	}
	        pm.close();
	    }
        if (pmf != null) {
            pmf.close();
        }
	}


	public static ZooJdoProperties getProps() {
		return new ZooJdoProperties(DB_NAME);
	}
	
	
	public static void defineIndex(String dbName, Class<?> cls, String fieldName, 
			boolean isUnique) {
		Properties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = null;
		try {
			pm = pmf.getPersistenceManager();
			pm.currentTransaction().begin();

			ZooClass s = ZooSchema.locateClass(pm, cls);
			s.getField(fieldName).createIndex(isUnique);

			pm.currentTransaction().commit();
		} finally {
			safeClose(pmf, pm);
		}
	}
	
	public static void defineIndex(Class<?> cls, String fieldName, boolean isUnique) {
		defineIndex(DB_NAME, cls, fieldName, isUnique);
	}

	/**
	 * Reflection tool to get direct access to Java fields.
	 * @param fName
	 * @param obj
	 * @return The value of the field. Primitives are auto-boxed into according instances.
	 */
	public static Object getFieldValue(String fName, Object obj) {
		try {
			Class<?> cls = obj.getClass();
			Field f = cls.getDeclaredField(fName);
			f.setAccessible(true);
			return f.get(obj);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException(e);
		} catch (SecurityException e) {
			throw new RuntimeException(e);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	
}
