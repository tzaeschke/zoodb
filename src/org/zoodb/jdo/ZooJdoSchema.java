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
package org.zoodb.jdo;

import java.util.Collection;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.impl.PersistenceManagerImpl;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooHandle;
import org.zoodb.schema.ZooSchema;


/**
 * Public factory class to manage database schemata.
 * 
 * @author ztilmann
 */
public final class ZooJdoSchema {

	private ZooJdoSchema() {
	}
	
	/**
	 * Define a new database class schema based on the given Java class.
	 * @param pm
	 * @param cls
	 * @return New schema object
	 * @see ZooSchema#defineClass(Class)
	 */
	public static ZooClass defineClass(PersistenceManager pm, Class<?> cls) {
		return getSchemaManager(pm).defineClass(cls);
	}

	/**
	 * Locate the class definition for the given class.
	 * @param pm
	 * @param cls
	 * @return The class definition or {@code null} if the class is not defined in the database
	 * @see ZooSchema#locateClass(Class)
	 */
	public static ZooClass locateClass(PersistenceManager pm, Class<?> cls) {
		return getSchemaManager(pm).locateClass(cls);
	}

	/**
	 * Locate the class definition for the given class.
	 * @param pm
	 * @param className
	 * @return The class definition or {@code null} if the class is not defined in the database
	 * @see ZooSchema#locateClass(String)
	 */
	public static ZooClass locateClass(PersistenceManager pm, String className) {
		return getSchemaManager(pm).locateClass(className);
	}

	/**
	 * This declares a new database class schema. This method creates an empty class
	 * with no attributes. It does not consider any existing Java classes of the same name.  
	 * @param pm
	 * @param className
	 * @return New schema object
	 * @see ZooSchema#defineEmptyClass(String)
	 */
	public static ZooClass defineEmptyClass(PersistenceManager pm, String className) {
		return getSchemaManager(pm).defineEmptyClass(className);
	}
	
	/**
	 * Declares a new class with a given super-class. The new class contains no attributes
	 * except attributes derived from the super class. This method does not consider any existing 
	 * Java classes of the same name.  
	 * @param pm
	 * @param className
	 * @param superCls
	 * @return New schema object
	 * @see ZooSchema#defineEmptyClass(String, ZooClass)
	 */
	public static ZooClass defineEmptyClass(PersistenceManager pm, String className,
			ZooClass superCls) {
		return getSchemaManager(pm).defineEmptyClass(className, superCls);
	}
	
	/**
	 * Get a Handle for the object with the specified object identifier.
	 * @param pm
	 * @param oid Object identifier.
	 * @return handle
	 * @see ZooSchema#getHandle(long)
	 */
	public static ZooHandle getHandle(PersistenceManager pm, long oid) {
		return getSchemaManager(pm).getHandle(oid);
	}

	/**
	 * Get a list of all user-defined classes in the database.
	 * @param pm
	 * @return a list of all user-defined classes in the database
	 * @see ZooSchema#locateAllClasses()
	 */
    public static Collection<ZooClass> locateAllClasses(PersistenceManager pm) {
        return getSchemaManager(pm).locateAllClasses();
    }
    
    private static ZooSchema getSchemaManager(PersistenceManager pm) {
    	return ((PersistenceManagerImpl)pm).getSession().schema();
    }
}
