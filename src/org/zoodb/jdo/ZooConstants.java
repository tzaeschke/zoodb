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
package org.zoodb.jdo;

/**
 * This class holds property strings for properties that are specific to ZooDB.
 * 
 * @author Tilmann Zäschke
 */
public class ZooConstants {

	/**
	 * Property that defines whether schemata should be created as necessary or need explicit
	 * creation. Default is false.
	 */
	public static final String PROPERTY_AUTO_CREATE_SCHEMA = "zoodb.autoCreateSchema";

	/**
	 * Property that defines whether evict() should also reset primitive values. By default, 
	 * ZooDB only resets references to objects, even though the JDO spec states that all fields
	 * should be evicted. 
	 * In a properly enhanced/activated class, the difference should no be noticeable, because
	 * access to primitive fields of evicted objects should always trigger a reload. Because of 
	 * this, ZooDB by default avoids the effort of resetting primitive fields.
	 * Default is false.
	 */
	public static final String PROPERTY_EVICT_PRIMITIVES = "zoodb.evictPrimitives";

	
}
