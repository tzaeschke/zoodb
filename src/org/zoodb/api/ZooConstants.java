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
package org.zoodb.api;

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


	/**
	 * Property that defines how access to closed Queries and Extent should be handled. 
	 * Queries and Extents are automatically closed at transaction boundaries.
	 * y default, as specified in JDO 3.1, closed queries and extents behave as if they were
	 * empty.
	 * 
	 * ZooDB allows to change this behavior such that access to closed Queries and Extents
	 * cause an Exception to be thrown. This may be desirable because it can indicate
	 * erroneous access to invalidated queries and extents, suggesting that they
	 * were fully traversed, rather than indicating that the result appears only empty because
	 * it is accessed at the wrong time.
	 * Default is {@code false}.
	 */
	public static final String PROPERTY_FAIL_ON_CLOSED_QUERIES = "zoodb.failOnClosedQueries";
	
}
