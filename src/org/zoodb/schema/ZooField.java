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




/**
 * Public interface to manage database schema class fields.
 * 
 * @author ztilmann
 */
public interface ZooField {

	public abstract void remove();

	/**
	 * Creates an index on the specified field for the current class and all sub-classes.
	 * @param isUnique
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
	 * @param hdl
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
