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




/**
 * Public interface to manage database schema class fields.
 * 
 * @author ztilmann
 */
public interface ZooField {

	public abstract void remove();

	/**
	 * Creates an index on the specified field for the current class and all sub-classes.
	 * @param isUnique Whether the index should be an index that enforces unique keys.
	 */
	public abstract void createIndex(boolean isUnique);

	public abstract boolean removeIndex();

	public abstract boolean hasIndex();

	public abstract boolean isIndexUnique();


	public abstract void rename(String name);

	/**
	 * 
	 * @return The name of the Java class of this schema.
	 */
	public abstract String getName();

	public abstract String getTypeName();

	/**
	 * Get the value of a given field.
	 * Returns Object Identifiers in case of references.
	 * 
	 * @param hdl The object handle
	 * @return The value of that field.
	 */
    public abstract Object getValue(ZooHandle hdl);

    public abstract void setValue(ZooHandle hdl, Object val);

    /**
     * If this field represents an array, then this method returns the dimensions of the array,
     * otherwise it returns 0.
     * @return Dimensions of the array or 0 if this is not an array.
     */
	public abstract int getArrayDim();

}
