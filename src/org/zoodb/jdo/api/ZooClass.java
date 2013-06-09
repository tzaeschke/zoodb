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
package org.zoodb.jdo.api;

import java.util.Iterator;
import java.util.List;



/**
 * Public interface to manage database class schemata.
 * 
 * @author ztilmann
 */
public interface ZooClass {

	public abstract Class<?> getJavaClass();

	public abstract void remove();

	public abstract void removeWithSubClasses();

	/**
	 * Short for locateField(name).createIndex(). 
	 */
	public abstract void createIndex(String fieldName, boolean isUnique);

	/**
	 * Short for locateField(name).removeIndex(). 
	 */
	public abstract boolean removeIndex(String fieldName);

	/**
	 * Short for locateField(name).hasIndex(). 
	 */
	public abstract boolean hasIndex(String fieldName);

	/**
	 * Short for locateField(name).isIndexUnique(). 
	 */
	public abstract boolean isIndexUnique(String fieldName);

	/**
	 * Drops all instances of the class. This does not affect cached instances
	 */
	public abstract void dropInstances();

	public abstract void rename(String name);

	/**
	 * 
	 * @return The name of the Java class of this schema.
	 */
	public abstract String getName();

	public abstract ZooClass getSuperClass();

	/**
	 * @return All fields declared in this class, excluding fields of super classes.
	 */
	public abstract List<ZooField> getLocalFields();

	/**
	 * @return All fields, including fields of super classes.
	 */
	public abstract List<ZooField> getAllFields();

	/**
	 * Adds a new field to this class.
	 * @param fieldName
	 * @param type
	 * @return 
	 */
	public abstract ZooField defineField(String fieldName, Class<?> type);

	/**
	 * Adds a new field to this class. The type is a reference (or array of references) to a 
	 * persistent type. Use arrayDimensions to specify the dimensionality of the array or 0 to
	 * indicate a non-array reference.
	 * 
	 * @param fieldName
	 * @param type
	 * @param arrayDimensions
	 */
	public abstract ZooField defineField(String fieldName, ZooClass type, int arrayDimensions);

	public abstract ZooField getField(String fieldName);

	public abstract void removeField(String fieldName);

	public abstract void removeField(ZooField field);

	public abstract List<ZooClass> getSubClasses();

	/**
	 * @return Iterator over materialized instances of the given class. The database schema must
	 * match the JVM class, otherwise an exception is thrown.
	 */
	public abstract Iterator<?> getInstanceIterator();

	/**
	 * Returns ZooHandles for according instances in the database. This method does not consider
	 * new or modified objects in the cache.
	 * 
	 * @param subClasses Specify whether sub-classes should be included.
	 * 
	 * @return Iterator over handles of the given class. 
	 */
	public abstract Iterator<ZooHandle> getHandleIterator(boolean subClasses);

	/**
	 * Returns the number of instances in the database. This operation ignores modifications
	 * of the current transactions, such as new or deleted instances. In other words, only
	 * committed objects are considered.
	 * 
	 * @param b Whether sub-classes should be counted as well. 
	 *  
	 * @return Number of instances in the database.
	 */
	public abstract long instanceCount(boolean subClasses);
	
	public abstract ZooHandle newInstance();
	
	public abstract ZooHandle newInstance(long oid);
}
