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

/**
 * This class holds property strings for properties that are specific to ZooDB.
 * 
 * @author Tilmann Zaeschke
 */
public class ZooConstants {

//	/**
//	 * Property that defines whether ZooDB should allow second class objects (= embedded objects)
//	 * of types other than the ones allowed by JDO.
//	 * For example, objects of type {@code String} are embedded objects in JDO, which means that
//	 * they can be referenced from persistent classes and can be stored and restored without
//	 * having to be a persistent class themselves.
//	 * 
//	 * However, for example instances of class {@code File} are by default not allowed and will 
//	 * cause an exception (instances of {@code File} are also not recommended to be stored). 
//	 * Setting this flag to {@code true} allows almost all classes to become embedded objects.
//	 *  
//	 * Default is {@code false}.
//	 */
//	public static final String PROPERTY_ALLOW_NON_STANDARD_SCOS = "zoodb.allowAllSCOs";

	/**
	 * Property that defines whether schemata should be created as necessary or need explicit
	 * creation. Default is {@code true}.
	 */
	public static final String PROPERTY_AUTO_CREATE_SCHEMA = "zoodb.autoCreateSchema";

	/**
	 * Property that defines whether {@code evict()} should also reset primitive values. By default, 
	 * ZooDB only resets references to objects, even though the JDO spec states that all fields
	 * should be evicted. 
	 * In a properly enhanced/activated class, the difference should no be noticeable, because
	 * access to primitive fields of evicted objects should always trigger a reload. Because of 
	 * this, ZooDB by default avoids the effort of resetting primitive fields.
	 * Default is {@code false}.
	 */
	public static final String PROPERTY_EVICT_PRIMITIVES = "zoodb.evictPrimitives";

	
}
