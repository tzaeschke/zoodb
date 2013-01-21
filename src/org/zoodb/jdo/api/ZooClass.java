/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooHandle;


/**
 * Public interface to manage database class schemata.
 * 
 * @author ztilmann
 */
public abstract class ZooClass {

	public abstract Class<?> getJavaClass();

	public abstract void remove();

	public abstract void defineIndex(String fieldName, boolean isUnique);

	public abstract boolean removeIndex(String fieldName);

	public abstract boolean isIndexDefined(String fieldName);

	public abstract boolean isIndexUnique(String fieldName);

	public static ZooHandle getHandle(PersistenceManager pm, long oid) {
		return Session.getSession(pm).getHandle(oid);
	}

	/**
	 * Drops all instances of the class. This does not affect cached instances
	 */
	public abstract void dropInstances();

	public abstract void rename(String name);

	/**
	 * 
	 * @return The name of the Java class of this schema.
	 */
	public abstract String getClassName();

	public abstract ZooClass getSuperClass();

	public abstract ZooField[] getFields();

	/**
	 * Adds a new field to this class.
	 * @param fieldName
	 * @param type
	 * @return 
	 */
	public abstract ZooField declareField(String fieldName, Class<?> type);

	/**
	 * Adds a new field to this class. The type is a reference (or array of references) to a 
	 * persistent type. Use arrayDimensions to specify the dimensionality of the array or 0 to
	 * indicate a non-array reference.
	 * 
	 * @param fieldName
	 * @param type
	 * @param arrayDimensions
	 */
	public abstract ZooField declareField(String fieldName, ZooClass type, int arrayDimensions);

}
