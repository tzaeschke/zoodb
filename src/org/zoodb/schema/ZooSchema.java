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
package org.zoodb.schema;

import java.util.Collection;

/**
 * Public factory class to manage database schemata.
 * 
 * @author ztilmann
 */
public interface ZooSchema {

	/**
	 * Define a new database class schema based on the given Java class.
	 * @param cls
	 * @return New schema object
	 */
	public ZooClass addClass(Class<?> cls);

	/**
	 * Locate the class definition for the given class.
	 * @param cls
	 * @return The class definition or {@code null} if the class is not defined in the database
	 */
	public ZooClass getClass(Class<?> cls);

	/**
	 * Locate the class definition for the given class.
	 * @param className
	 * @return The class definition or {@code null} if the class is not defined in the database
	 */
	public ZooClass getClass(String className);

	/**
	 * This declares a new database class schema. This method creates an empty class
	 * with no attributes. It does not consider any existing Java classes of the same name.  
	 * @param className
	 * @return New schema object
	 */
	public ZooClass defineEmptyClass(String className);
	
	/**
	 * Declares a new class with a given super-class. The new class contains no attributes
	 * except attributes derived from the super class. This method does not consider any existing 
	 * Java classes of the same name.  
	 * @param className
	 * @param superCls
	 * @return New schema object
	 */
	public ZooClass defineEmptyClass(String className, ZooClass superCls);

	/**
	 * Get a Handle for the object with the specified object identifier.
	 * @param oid Object identifier.
	 * @return handle
	 */
	public ZooHandle getHandle(long oid);

	/**
	 * Get a Handle for the specified persistent object.
	 * @param pc Persistent capable object.
	 * @return handle
	 */
	public ZooHandle getHandle(Object pc);

	/**
	 * Get a list of all user-defined classes in the database.
	 * @return a list of all user-defined classes in the database
	 */
    public Collection<ZooClass> getAllClasses();

}
