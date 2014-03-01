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
package org.zoodb.schema;

import java.util.Collection;

import javax.jdo.PersistenceManager;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.client.SchemaManager;
import org.zoodb.jdo.impl.PersistenceManagerImpl;


/**
 * Public factory class to manage database schemata.
 * 
 * @author ztilmann
 */
public final class ZooSchema {

	private ZooSchema() {
	}
	
	/**
	 * Define a new database class schema based on the given Java class.
	 * @param pm
	 * @param cls
	 * @return New schema object
	 */
	public static ZooClass defineClass(PersistenceManager pm, Class<?> cls) {
    	checkValidity(pm);
		return getSchemaManager(pm).createSchema(null, cls);
	}

	/**
	 * Locate the class definition for the given class.
	 * @param pm
	 * @param cls
	 * @return The class definition or {@code null} if the class is not defined in the database
	 */
	public static ZooClass locateClass(PersistenceManager pm, Class<?> cls) {
    	checkValidity(pm);
		return getSchemaManager(pm).locateSchema(cls, null);
	}

	/**
	 * Locate the class definition for the given class.
	 * @param pm
	 * @param className
	 * @return The class definition or {@code null} if the class is not defined in the database
	 */
	public static ZooClass locateClass(PersistenceManager pm, String className) {
    	checkValidity(pm);
		return getSchemaManager(pm).locateSchema(className);
	}

	/**
	 * This declares a new database class schema. This method creates an empty class
	 * with no attributes. It does not consider any existing Java classes of the same name.  
	 * @param pm
	 * @param className
	 * @return New schema object
	 */
	public static ZooClass defineEmptyClass(PersistenceManager pm, String className) {
    	checkValidity(pm);
    	if (!checkJavaClassNameConformity(className)) {
    		throw new IllegalArgumentException("Not a valid class name: \"" + className + "\"");
    	}
		return getSchemaManager(pm).declareSchema(className, null);
	}
	
	/**
	 * Declares a new class with a given super-class. The new class contains no attributes
	 * except attributes derived from the super class. This method does not consider any existing 
	 * Java classes of the same name.  
	 * @param pm
	 * @param className
	 * @param superCls
	 * @return New schema object
	 */
	public static ZooClass defineEmptyClass(PersistenceManager pm, String className, 
			ZooClass superCls) {
    	checkValidity(pm);
    	if (!checkJavaClassNameConformity(className)) {
    		throw new IllegalArgumentException("Not a valid class name: \"" + className + "\"");
    	}
		return getSchemaManager(pm).declareSchema(className, superCls);
	}
	
	private static boolean checkJavaClassNameConformity(String className) {
		if (className == null || className.length() == 0) {
			return false;
		}
		for (int i = 0; i < className.length(); i++) {
			char c = className.charAt(i);
			if (i == 0) {
				if (!Character.isJavaIdentifierStart(c)) {
					return false;
				}
			} else {
				if (c != '.' && !Character.isJavaIdentifierPart(c)) {
					return false;
				}
			}
		}
		
		//check existing class. For now we disallow class names of non-persistent classes.
		try {
			Class<?> cls = Class.forName(className);
			if (!ZooPCImpl.class.isAssignableFrom(cls)) {
				throw new IllegalArgumentException("Class is not persistence capable: " + cls);
			}
		} catch (ClassNotFoundException e) {
			//okay, class not found.
		}
		
		return true;
	}
	
	public static ZooHandle getHandle(PersistenceManager pm, long oid) {
    	checkValidity(pm);
		return ((PersistenceManagerImpl)pm).getSession().getHandle(oid);
	}

    public static Collection<ZooClass> locateAllClasses(PersistenceManager pm) {
    	checkValidity(pm);
        return getSchemaManager(pm).getAllSchemata();
    }
    
    private static void checkValidity(PersistenceManager pm) {
    	if (pm.isClosed()) {
    		throw new IllegalStateException("PersistenceManager is closed.");
    	}
    	if (!pm.currentTransaction().isActive()) {
    		throw new IllegalStateException("Transaction is closed. Missing 'begin()' ?");
    	}
    }

    private static SchemaManager getSchemaManager(PersistenceManager pm) {
    	return ((PersistenceManagerImpl)pm).getSession().getSchemaManager();
    }
}
