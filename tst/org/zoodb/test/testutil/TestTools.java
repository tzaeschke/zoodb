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
package org.zoodb.test.testutil;

import java.lang.reflect.Field;
import java.util.Properties;

import javax.jdo.JDOException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.tools.ZooDebug;
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
	 * @param classes classes
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
	        	ZooJdoHelper.schema(pm).addClass(cls);
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
            	ZooClass c = ZooJdoHelper.schema(pm).getClass(cls);
            	if (c != null) {
            		c.remove();
            	}
            }
            pm.currentTransaction().commit();
        } finally {
			safeClose(pmf, pm);
        }
	}


	public static PersistenceManager openPM() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		//for the tests we prefer manual creation
		props.setZooAutoCreateSchema(false);
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
			}
		}
		try {
			pmf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
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

	/**
	 * 
	 * @return Full path of the default DB.
	 */
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
	        	ZooJdoHelper.schema(pm).getClass(cls).dropInstances();
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

			ZooClass s = ZooJdoHelper.schema(pm).getClass(cls);
			s.getField(fieldName).createIndex(isUnique);

			pm.currentTransaction().commit();
		} finally {
			safeClose(pmf, pm);
		}
	}
	
	public static void defineIndex(Class<?> cls, String fieldName, boolean isUnique) {
		defineIndex(DB_NAME, cls, fieldName, isUnique);
	}

	public static void removeIndex(String dbName, Class<?> cls, String fieldName) {
		Properties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = null;
		try {
			pm = pmf.getPersistenceManager();
			pm.currentTransaction().begin();

			ZooClass s = ZooJdoHelper.schema(pm).getClass(cls);
			s.getField(fieldName).removeIndex();

			pm.currentTransaction().commit();
		} finally {
			safeClose(pmf, pm);
		}
	}
	
	public static void removeIndex(Class<?> cls, String fieldName) {
		removeIndex(DB_NAME, cls, fieldName);
	}

	/**
	 * Reflection tool to get direct access to Java fields.
	 * @param fName field name
	 * @param obj object
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
