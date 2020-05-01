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
	 * @param cls Class to add
	 * @return New schema object
	 */
	public ZooClass addClass(Class<?> cls);

	/**
	 * Locate the class definition for the given class.
	 * @param cls Class to get schema for
	 * @return The class definition or {@code null} if the class is not defined in the database
	 */
	public ZooClass getClass(Class<?> cls);

	/**
	 * Locate the class definition for the given class.
	 * @param className Class name to get schema for
	 * @return The class definition or {@code null} if the class is not defined in the database
	 */
	public ZooClass getClass(String className);

	/**
	 * This declares a new database class schema. This method creates an empty class
	 * with no attributes. It does not consider any existing Java classes of the same name.  
	 * @param className Class name
	 * @return New schema object
	 */
	public ZooClass defineEmptyClass(String className);
	
	/**
	 * Declares a new class with a given super-class. The new class contains no attributes
	 * except attributes derived from the super class. This method does not consider any existing 
	 * Java classes of the same name.  
	 * @param className Class name
	 * @param superCls Super class, or {@code null} for none
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
