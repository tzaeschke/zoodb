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
package org.zoodb.jdo.internal.client;

import java.util.ArrayList;
import java.util.Collection;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.internal.ZooClassProxy;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;

/**
 * This class maps schema data between the external Schema/ISchema classes and
 * the internal ZooClassDef class. 
 * 
 * @author Tilmann Zaeschke
 *
 */
public class SchemaManager {

	private ClientSessionCache cache;
	private final ArrayList<SchemaOperation> ops = new ArrayList<SchemaOperation>();

	public SchemaManager(ClientSessionCache cache) {
		this.cache = cache;
	}
	
	public boolean isSchemaDefined(Class<?> cls, Node node) {
		return (locateClassDefinition(cls, node) != null);
	}

	/**
	 * Checks class and disk for class definition.
	 * @param cls
	 * @param node
	 * @return Class definition, may return null if no definition is found.
	 */
	private ZooClassDef locateClassDefinition(Class<?> cls, Node node) {
		ZooClassDef def = cache.getSchema(cls, node);
		if (def != null) {
			//return null if deleted
			if (!def.jdoZooIsDeleted()) { //TODO load if hollow???
				return def;
			}
			return null;
		}
		
		return null;
	}

	public ZooClassProxy locateSchema(Class<?> cls, Node node) {
		ZooClassDef def = locateClassDefinition(cls, node);
		//not in cache and not on disk
		if (def == null) {
			return null;
		}
		//it should now be in the cache
		//return a unique handle, even if called multiple times. There is currently
		//no real reason, other than that it allows == comparison.
		return def.getVersionProxy();
	}

	public void refreshSchema(ZooClassDef def) {
		def.jdoZooGetNode().refreshSchema(def);
	} 
	
	private ZooClassDef locateClassDefinition(String clsName) {
		ZooClassDef def = cache.getSchema(clsName);
		if (def != null) {
			//return null if deleted
			if (!def.jdoZooIsDeleted()) { //TODO load if hollow???
				return def;
			}
			return null;
		}
		
		return def;
	}

	public ZooClassProxy locateSchema(String className) {
		ZooClassDef def = locateClassDefinition(className);
		//not in cache and not on disk
		if (def == null) {
			return null;
		}
		//it should now be in the cache
        return getSchemaProxy(def);
	}

	public ZooClassProxy locateSchemaForObject(long oid, Node node) {
		ZooPCImpl pc = cache.findCoByOID(oid);
		if (pc != null) {
			return pc.jdoZooGetClassDef().getVersionProxy();
		}
		
		//object not loaded or instance of virtual class
		//so we don't fully load the object, but only get its schema
		ZooClassDef def = cache.getSchema(node.getSchemaForObject(oid));
		return getSchemaProxy(def);
	}

	private ZooClassProxy getSchemaProxy(ZooClassDef def) {
        //return a unique handle, even if called multiple times. There is currently
        //no real reason, other than that it allows == comparison.
        return def.getVersionProxy();
	}
	
	public ZooClassProxy createSchema(Node node, Class<?> cls) {
		if (isSchemaDefined(cls, node)) {
			throw new JDOUserException(
					"Schema is already defined: " + cls.getName());
		}
		//Is this PersistentCapanbleImpl or a sub class?
		if (!(ZooPCImpl.class.isAssignableFrom(cls))) {
			throw new JDOUserException(
						"Class has no persistent capable super class: " + cls.getName());
		}
        if (cls.isMemberClass()) {
        	System.err.println("ZooDB - Found innner class: " + cls.getName());
            throw new JDOUserException(
                    "Member (non-static inner) classes are not permitted: " + cls.getName());
        }
        if (cls.isLocalClass()) {
            throw new JDOUserException(
                    "Local classes (defined in a method) are not permitted: " + cls.getName());
        }
        if (cls.isAnonymousClass()) {
            throw new JDOUserException(
                    "Anonymous classes are not permitted: " + cls.getName());
        }
        if (cls.isInterface()) {
            throw new JDOUserException(
                    "Interfaces are currently not supported: " + cls.getName());
        }

		ZooClassDef def;
		if (cls != ZooPCImpl.class) {
			Class<?> clsSuper = cls.getSuperclass();
			ZooClassDef defSuper = locateClassDefinition(clsSuper, node);
			def = ZooClassDef.createFromJavaType(cls, defSuper, node, cache.getSession()); 
		} else {
			def = ZooClassDef.createFromJavaType(cls, null, node, cache.getSession());
		}
		cache.addSchema(def, false, node);
		ops.add(new SchemaOperation.SchemaDefine(def));
		return def.getVersionProxy();
	}

	public void deleteSchema(ZooClassProxy proxy, boolean deleteSubClasses) {
		if (!deleteSubClasses && !proxy.getSubProxies().isEmpty()) {
		    throw new IllegalStateException("Can not remove class schema while sub-classes are " +
		            " still defined: " + proxy.getSubProxies().get(0).getName());
		}
		if (proxy.getSchemaDef().jdoZooIsDeleted()) {
			throw new JDOObjectNotFoundException("This objects has already been deleted.");
		}
		
		if (deleteSubClasses) {
			while (!proxy.getSubProxies().isEmpty()) {
				deleteSchema(proxy.getSubProxies().get(0), true);
			}
		}
		
		//delete instances
		for (ZooPCImpl pci: cache.getAllObjects()) {
			if (pci.jdoZooGetClassDef().getSchemaId() == proxy.getSchemaId()) {
				pci.jdoZooMarkDeleted();
			}
		}
		// Delete whole version tree
		ops.add(new SchemaOperation.SchemaDelete(proxy));
		for (ZooClassDef def: proxy.getAllVersions()) {
			def.jdoZooMarkDeleted();
		}
	}

	public void defineIndex(ZooFieldDef f, boolean isUnique) {
		if (f.isIndexed()) {
			throw new JDOUserException("Field is already indexed: " + f.getName());
		}
		ops.add(new SchemaOperation.IndexCreate(f, isUnique));
	}

	public boolean removeIndex(ZooFieldDef f) {
		if (!f.isIndexed()) {
			return false;
		}
		ops.add(new SchemaOperation.IndexRemove(f));
		return true;
	}

	public boolean isIndexDefined(ZooFieldDef f) {
		return f.isIndexed();
	}

	public boolean isIndexUnique(ZooFieldDef f) {
		if (!f.isIndexed()) {
			throw new IllegalStateException("Field has no index: " + f.getName());
		}
		return f.isIndexUnique();
	}
	
	public void commit() {
		// perform pending operations
		for (SchemaOperation op: ops) {
			op.commit();
		}
		ops.clear();
	}

	public void rollback() {
		// undo pending operations - to be rolled back in reverse order
		for (int i = ops.size()-1; i >= 0; i--) {
			ops.get(i).rollback();
		}
		ops.clear();
	}

	public Object dropInstances(ZooClassProxy def) {
		ops.add(new SchemaOperation.DropInstances(def));
		return true;
	}

	public void renameSchema(ZooClassDef def, String newName) {
		if (cache.getSchema(newName) != null) {
			throw new IllegalStateException("Class name is already in use: " + newName);
		}
		ops.add(new SchemaOperation.SchemaRename(cache, def, newName));
	}

    public Collection<ZooClass> getAllSchemata() {
        ArrayList<ZooClass> list = new ArrayList<ZooClass>();
        for (ZooClassDef def: cache.getSchemata()) {
            if (!def.jdoZooIsDeleted()) {
                list.add( getSchemaProxy(def) );
            }
        }
        return list;
    }

	public ZooClass declareSchema(String className, ZooClass superCls, Node node) {
		if (locateClassDefinition(className) != null) {
			throw new IllegalArgumentException("This class is already defined: " + className);
		}
		
		long oid = node.getOidBuffer().allocateOid();
		
		ZooClassDef defSuper;
		if (superCls != null) {
			defSuper = ((ZooClassProxy)superCls).getSchemaDef();
		} else {
			defSuper = locateClassDefinition(ZooPCImpl.class, node);
		}
		ZooClassDef def = ZooClassDef.declare(className, oid, defSuper.getOid());
		def.associateSuperDef(defSuper);
		def.associateProxy(new ZooClassProxy(def, cache.getSession()));
		def.associateFields();
		
		cache.addSchema(def, false, node);
		ops.add(new SchemaOperation.SchemaDefine(def));
		return def.getVersionProxy();
	}

	public ZooFieldDef addField(ZooClassDef def, String fieldName, Class<?> type) {
		def = def.getModifiableVersion(cache, ops);
		long fieldOid = def.jdoZooGetNode().getOidBuffer().allocateOid();
		ZooFieldDef field = ZooFieldDef.create(def, fieldName, type, fieldOid);
		ops.add(new SchemaOperation.SchemaFieldDefine(def, field));
		return field;
	}

	public ZooFieldDef addField(ZooClassDef def, String fieldName, ZooClassDef typeDef, 
			int arrayDim) {
		def = def.getModifiableVersion(cache, ops);
		ZooFieldDef field = ZooFieldDef.create(def, fieldName, typeDef, arrayDim);
		ops.add(new SchemaOperation.SchemaFieldDefine(def, field));
		return field;
	}

	public ZooClassDef removeField(ZooFieldDef field) {
		ZooClassDef def = field.getDeclaringType().getModifiableVersion(cache, ops);
		//new version -- new field
		field = def.getField(field.getName()); 
		ops.add(new SchemaOperation.SchemaFieldDelete(def, field));
		return def;
	}

	public void renameField(ZooFieldDef field, String fieldName) {
		//We do not create a new version just for renaming.
		ZooClassDef def = field.getDeclaringType();
		ops.add(new SchemaOperation.SchemaFieldRename(field, fieldName));
		def.jdoZooMarkDirty();
	}
}
