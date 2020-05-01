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
	 * Creates an index on the specified field for the current class and all sub-classes.
	 * Short for {@link ZooClass#getField(String)}.createIndex(). 
	 * @param fieldName The name of the field to be indexed
	 * @param isUnique Whether the index should be an index that enforces unique keys.
	 */
	public abstract void createIndex(String fieldName, boolean isUnique);

	/**
	 * Short for {@link ZooClass#getField(String)}.removeIndex(). 
	 * @param fieldName The name of the field where the index should be removed
	 * @return {@code true} if the index could be removed
	 */
	public abstract boolean removeIndex(String fieldName);

	/**
	 * Short for {@link ZooClass#getField(String)}.hasIndex(). 
	 * @param fieldName The name of the field to check
	 * @return {@code true} if the field has and index
	 */
	public abstract boolean hasIndex(String fieldName);

	/**
	 * Short for {@link ZooClass#getField(String)}.isIndexUnique(). 
	 * @param fieldName The name of the field whose index should be checked
	 * @return {@code true} if the index is unique
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
	 * @param fieldName The name of the field to be added
	 * @param type The Java type of the new field
	 * @return Field schema object for the given field name
	 */
	public abstract ZooField addField(String fieldName, Class<?> type);

	/**
	 * Adds a new field to this class. The type is a reference (or array of references) to a 
	 * persistent type. Use arrayDimensions to specify the dimensionality of the array or 0 to
	 * indicate a non-array reference.
	 * 
	 * @param fieldName The name of the field to be added
	 * @param type The schema type of the new field
	 * @param arrayDimensions the number of dimensions of the array
	 * @return The new Field instance
	 */
	public abstract ZooField addField(String fieldName, ZooClass type, int arrayDimensions);

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
	 * @param subClasses Whether sub-classes should be counted as well. 
	 *  
	 * @return Number of instances in the database.
	 */
	public abstract long instanceCount(boolean subClasses);
	
	public abstract ZooHandle newInstance();
	
	public abstract ZooHandle newInstance(long oid);
}
