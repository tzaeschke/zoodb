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
import java.util.List;

import javax.jdo.JDOUserException;

import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.internal.client.SchemaManager;
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
	private final Node node;
	private final SchemaManager schemaManager;
	
	public SchemaClassProxy(ZooClassDef def, Class<?> cls, Node node, 
			SchemaManager schemaManager) {
		this.def = def;
		this.node = node;
		this.schemaManager = schemaManager;
	}
	
	@Override
	public void remove() {
		checkInvalid();
		schemaManager.deleteSchema(this);
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
		//update to latest version, just in case super class got modified or something...
		getLatestVersion(false);
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
		ZooFieldDef field = ZooFieldDef.create(def, fieldName, type);
		schemaManager.addField(def, field, node);
		//Update, in case it changed
		def = field.getDeclaringType();
		return field.getApiHandle();
	}

	@Override
	public ZooField declareField(String fieldName, ZooClass type, int arrayDepth) {
		checkAddField(fieldName);
		ZooClassDef typeDef = ((SchemaClassProxy)type).getSchemaDef();
		ZooFieldDef field = ZooFieldDef.create(def, fieldName, typeDef, arrayDepth);
		schemaManager.addField(def, field, node);
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
		getLatestVersion(true);
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
			System.out.println("LF: " + f.getName() + "  " + f.getDeclaringType().getClassName() + " " + f.getDeclaringType().getOid());
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
		getLatestVersion(true);
		ZooFieldDef fieldDef = ((SchemaFieldProxy)field).getInternal();
		schemaManager.removeField(fieldDef, node);
		//Update, in case it changed
		def = fieldDef.getDeclaringType();

		getLatestVersion(true);
	}
	
	
	ZooClassDef getLatestVersion(boolean updateIntended) {
		ZooClassDef def1 = def;
		System.out.println("Proxy1: " + def);
		while (def.getNextVersion() != null) {
			def = def.getNextVersion();
			System.out.println("Proxy2: " + def);
		}
		if (def != def1) {
			//TODO remove this method
			//TODO remove this method
			//TODO remove this method
			//TODO remove this method
			throw new IllegalStateException();
		}
//		if (updateIntended && !def.jdoZooIsNew()) {
//			def = def.newVersion();
//		}
		return def;
	}

	public void updateVersion(ZooClassDef newDef) {
		def = newDef;
	}
}
