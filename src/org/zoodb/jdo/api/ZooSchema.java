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

import java.util.Collection;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooHandle;


/**
 * Public factory class to manage database schemata.
 * 
 * @author ztilmann
 */
public final class ZooSchema {

	private ZooSchema() {
	}
	
	/**
	 * Define a new database class schema based on the given Java class.
	 * @param pm
	 * @param cls
	 * @return New schema object
	 */
	public static ZooClass defineClass(PersistenceManager pm, Class<?> cls) {
		Node node = Session.getSession(pm).getPrimaryNode();
		return Session.getSession(pm).getSchemaManager().createSchema(node, cls);
	}

	public static ZooClass locateClass(PersistenceManager pm, Class<?> cls) {
		Node node = Session.getSession(pm).getPrimaryNode();
		return Session.getSession(pm).getSchemaManager().locateSchema(cls, node);
	}

	public static ZooClass locateClass(PersistenceManager pm, String className) {
		Node node = Session.getSession(pm).getPrimaryNode();
		return Session.getSession(pm).getSchemaManager().locateSchema(className, node);
	}

	/**
	 * This declares a new database class schema. This method does creates an empty class
	 * with no attributes. It does not consider any existing Java classes of the same name.  
	 * @param pm
	 * @param className
	 * @return New schema object
	 */
	public static ZooClass declareClass(PersistenceManager pm, String className) {
		Node node = Session.getSession(pm).getPrimaryNode();
		return Session.getSession(pm).getSchemaManager().declareSchema(className, null, node);
	}
	
	/**
	 * Declares a new class with a given super-class. 
	 * @param pm
	 * @param className
	 * @param superCls
	 * @return New schema object
	 */
	public static ZooClass declareClass(PersistenceManager pm, String className, 
			ZooClass superCls) {
		Node node = Session.getSession(pm).getPrimaryNode();
		return Session.getSession(pm).getSchemaManager().declareSchema(className, superCls, node);
	}
	
//	public static ZooClass defineClass(PersistenceManager pm, Class<?> cls, String nodeName) {
//		Node node = Session.getSession(pm).getNode(nodeName);
//		return Session.getSession(pm).getSchemaManager().createSchema(node, cls);
//	}
//
//	public static ZooClass locateClass(PersistenceManager pm, Class<?> cls, String nodeName) {
//		Node node = Session.getSession(pm).getNode(nodeName);
//		return Session.getSession(pm).getSchemaManager().locateSchema(cls, node);
//	}
//
//	public static ZooClass locateClass(PersistenceManager pm, String className,
//			String nodeName) {
//		Node node = Session.getSession(pm).getNode(nodeName);
//		return Session.getSession(pm).getSchemaManager().locateSchema(className, node);
//	}

	public static ZooHandle getHandle(PersistenceManager pm, long oid) {
		return Session.getSession(pm).getHandle(oid);
	}

    public static Collection<ZooClass> locateAllClasses(PersistenceManager pm) {
        Node node = Session.getSession(pm).getPrimaryNode();
        return Session.getSession(pm).getSchemaManager().getAllSchemata(node);
    }
}
