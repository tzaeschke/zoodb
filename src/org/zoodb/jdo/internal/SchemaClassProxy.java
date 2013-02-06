/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.jdo.JDOUserException;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.internal.client.SchemaManager;
import org.zoodb.jdo.internal.model1p.Node1P;
import org.zoodb.jdo.internal.util.ClassCreator;
import org.zoodb.jdo.internal.util.Util;

/**
 * Internal Schema class.
 * 
 * This should not throw any JDOxyz-exceptions, because it is not part of the JDO API.
 * 
 * The class serves as a proxy for the latest version of a particular class in the schema version
 * tree.
 * The proxy's reference to the latest version is updated by SchemaOperations.
 * 
 * @author Tilmann Zäschke
 */
public class SchemaClassProxy implements ZooClass {

	private ZooClassDef def;
	private final SchemaClassProxy defSuper;
	private final Node node;
	private final SchemaManager schemaManager;
	private final long schemaId;
	private ArrayList<SchemaClassProxy> subClasses = new ArrayList<SchemaClassProxy>();
	
	public SchemaClassProxy(ZooClassDef def, Node node, 
			SchemaManager schemaManager) {
		this.def = def;
		this.node = node;
		this.schemaManager = schemaManager;
		this.schemaId = def.getSchemaId();
		this.defSuper = def.getSuperDef().getApiHandle();
		if (defSuper == null && !def.getClassName().equals(ZooPCImpl.class.getName())) {
			throw new IllegalStateException();
		}
		this.defSuper.subClasses.add(this);
	}
	
	@Override
	public void remove() {
		checkInvalid();
		schemaManager.deleteSchema(this, false);
	}

	@Override
	public void removeWithSubClasses() {
		checkInvalid();
		schemaManager.deleteSchema(this, true);
	}

	public ZooClassDef getSchemaDef() {
		checkInvalid();
		return def;
	}

	public Node getNode() {
		checkInvalid();
		return node;
	}
	
	protected void checkInvalid() {
		Session s = def.getProvidedContext().getSession();
		if (s.getPersistenceManager().isClosed() || 
				!s.getPersistenceManager().currentTransaction().isActive()) {
			throw new IllegalStateException("This schema belongs to a closed PersistenceManager.");
		}
		if (def.jdoZooIsDeleted()) {
			throw new JDOUserException("This schema object is invalid, for " +
					"example because it has been deleted.");
		}
		
		//consistency check
		if (def.getNextVersion() != null) {
			throw new IllegalStateException();
		}
	}
	
	@Override
	public void defineIndex(String fieldName, boolean isUnique) {
		checkInvalid();
		schemaManager.defineIndex(fieldName, isUnique, node, def);
	}
	
	@Override
	public boolean removeIndex(String fieldName) {
		checkInvalid();
		return schemaManager.removeIndex(fieldName, node, def);
	}
	
	@Override
	public boolean isIndexDefined(String fieldName) {
		checkInvalid();
		return schemaManager.isIndexDefined(fieldName, node, def);
	}
	
	@Override
	public boolean isIndexUnique(String fieldName) {
		checkInvalid();
		return schemaManager.isIndexUnique(fieldName, node, def);
	}
	
	@Override
	public void dropInstances() {
		checkInvalid();
		schemaManager.dropInstances(node, def);
	}

	@Override
	public void rename(String newName) {
		checkInvalid();
		schemaManager.renameSchema(node, def, newName);
	}

	@Override
	public String getClassName() {
		checkInvalid();
		return def.getClassName();
	}

	@Override
	public ZooClass getSuperClass() {
		checkInvalid();
		return def.getSuperDef().getApiHandle();
	}

	@Override
	public List<ZooField> getAllFields() {
		checkInvalid();
		ArrayList<ZooField> ret = new ArrayList<ZooField>();
		for (ZooFieldDef fd: def.getAllFields()) {
			ret.add(fd.getApiHandle());
		}
		return ret;
	}

	@Override
	public List<ZooField> getLocalFields() {
		checkInvalid();
		ArrayList<ZooField> ret = new ArrayList<ZooField>();
		for (ZooFieldDef fd: def.getAllFields()) {
			if (fd.getDeclaringType() == def) {
				ret.add(fd.getApiHandle());
			}
		}
		return ret;
	}

	@Override
	public ZooField declareField(String fieldName, Class<?> type) {
		checkAddField(fieldName);
		ZooFieldDef field = schemaManager.addField(def, fieldName, type, node);
		//Update, in case it changed
		def = field.getDeclaringType();
		return field.getApiHandle();
	}

	@Override
	public ZooField declareField(String fieldName, ZooClass type, int arrayDepth) {
		checkAddField(fieldName);
		ZooClassDef typeDef = ((SchemaClassProxy)type).getSchemaDef();
		ZooFieldDef field = schemaManager.addField(def, fieldName, typeDef, arrayDepth, node);
		//Update, in case it changed
		def = field.getDeclaringType();
		return field.getApiHandle();
	}
	
	private void checkAddField(String fieldName) {
		checkInvalid();
		if (!checkJavaFieldNameConformity(fieldName)) {
			throw new IllegalArgumentException("Field name invalid: " + fieldName);
		}
		//check existing names
		for (ZooFieldDef fd: def.getAllFields()) {
			if (fd.getName().equals(fieldName)) {
				throw new IllegalArgumentException("Field name already defined: " + fieldName);
			}
		}
	}
	
	
	static boolean checkJavaFieldNameConformity(String fieldName) {
		if (fieldName == null || fieldName.length() == 0) {
			return false;
		}
		for (int i = 0; i < fieldName.length(); i++) {
			char c = fieldName.charAt(i);
			if (i == 0) {
				if (!Character.isJavaIdentifierStart(c)) {
					return false;
				}
			} else {
				if (!Character.isJavaIdentifierPart(c)) {
					return false;
				}
			}
		}
		return true;
	}
	

	
	@Override
	public String toString() {
		checkInvalid();
		return "Class schema(" + Util.getOidAsString(def) + "): " + def.getClassName();
	}

	@Override
	public Class<?> getJavaClass() {
        Class<?> cls = def.getJavaClass();
        if (cls == null) {
	        try {
	            cls = Class.forName(def.getClassName());
	        } catch (ClassNotFoundException e) {
	            cls = ClassCreator.createClass(
	            		def.getClassName(), def.getSuperDef().getClassName());
	            //throw new JDOUserException("Class not found: " + className, e);
	        }
        }
        return cls;
	}

	@Override
	public ZooField locateField(String fieldName) {
		checkInvalid();
		for (ZooFieldDef f: def.getAllFields()) {
			if (f.getName().equals(fieldName)) {
				return f.getApiHandle();
			}
		}
		return null;
	}
	
	@Override
	public void removeField(String fieldName) {
		checkInvalid();
		ZooField f = locateField(fieldName);
		if (f == null) {
			throw new IllegalStateException(
					"Field not found: " + def.getClassName() + "." + fieldName);
		}
		removeField(f);
	}

	@Override
	public void removeField(ZooField field) {
		checkInvalid();
		ZooFieldDef fieldDef = ((SchemaFieldProxy)field).getInternal();
		def = schemaManager.removeField(fieldDef, node);
	}
	
	public void newVersionRollback(ZooClassDef newDef) {
		if (def.getSchemaId() != schemaId) {
			//this would indicate a bug
			throw new IllegalArgumentException();
		}
		if (def != newDef 
				|| newDef.getPreviousVersion() == null 
				|| newDef.getNextVersion() != null) {
			//this would indicate a bug
			throw new IllegalStateException();
		}
		def = newDef.getPreviousVersion();
	}

	public void newVersion(ZooClassDef newDef) {
		if (def.getSchemaId() != schemaId) {
			//this would indicate a bug
			throw new IllegalArgumentException();
		}
		if (def == newDef 
				|| newDef.getPreviousVersion() != def 
				|| newDef.getNextVersion() != null) {
			//this would indicate a bug
			throw new IllegalStateException();
		}
		def = newDef;
	}

	@Override
	public List<ZooClass> getSubClasses() {
		checkInvalid();
		ArrayList<ZooClass> subs = new ArrayList<ZooClass>();
		for (SchemaClassProxy sub: subClasses) {
			subs.add(sub);
		}
		return subs;
	}

	@Override
	public Iterator<?> getInstanceIterator() {
		//TODO return CloseableIterator instead?
		return node.loadAllInstances(def, true);
	}

	public ArrayList<ZooClassDef> getAllVersions() {
		ArrayList<ZooClassDef> ret = new ArrayList<ZooClassDef>();
		ZooClassDef d = def;
		while (d != null) {
			ret.add(d);
			d = d.getPreviousVersion();
		}
		return ret;
	}

	public List<SchemaClassProxy> getSubProxies() {
		return subClasses;
	}

	/**
	 * 
	 * @return Schema ID which is independent of the schema version.
	 */
	public long getSchemaId() {
		return schemaId;
	}
}
