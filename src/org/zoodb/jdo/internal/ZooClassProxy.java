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
package org.zoodb.jdo.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.api.ZooHandle;
import org.zoodb.jdo.internal.client.SchemaManager;
import org.zoodb.jdo.internal.util.ClassCreator;
import org.zoodb.jdo.internal.util.IteratorTypeAdapter;
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
 * @author Tilmann Zaeschke
 */
public class ZooClassProxy implements ZooClass {

	private ZooClassDef def;
	private final ZooClassProxy superProxy;
	//TODO there should be only one proxy for all nodes, I guess...
	private final SchemaManager schemaManager;
	private final long schemaId;
	private ArrayList<ZooClassProxy> subClasses = new ArrayList<ZooClassProxy>();
	private final Session session;
	
	public ZooClassProxy(ZooClassDef def, Session session) {
		this.def = def;
		this.schemaManager = session.getSchemaManager();
		this.schemaId = def.getSchemaId();
		this.session = session;
		ZooClassDef defSuper = def.getSuperDef();
		if (!def.getClassName().equals(ZooPCImpl.class.getName())) {
			if (defSuper.getVersionProxy() == null) {
				//super-class needs a proxy
				this.superProxy = new ZooClassProxy(defSuper, session);
				defSuper.associateProxy(superProxy);
			} else {
				//super class already has a proxy
				this.superProxy = defSuper.getVersionProxy();
			}
			this.superProxy.subClasses.add(this);
			//associate previous versions
			while ((def = def.getPreviousVersion()) != null) {
				def.associateProxy(this);  
			}
		} else {
			// this is the root class
			superProxy = null;
		}
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
	
	protected void checkInvalid() {
		if (!session.isOpen()) {
			throw new IllegalStateException("This schema belongs to a closed PersistenceManager.");
		}
		if (def.jdoZooIsDeleted()) {
			throw new IllegalStateException("This schema object is invalid, for " +
					"example because it has been deleted.");
		}
		
		//consistency check
		if (def.getNextVersion() != null) {
			throw new IllegalStateException();
		}
	}
	
	@Override
	public void createIndex(String fieldName, boolean isUnique) {
		checkInvalid();
		locateFieldOrFail(fieldName).createIndex(isUnique);
	}
	
	@Override
	public boolean removeIndex(String fieldName) {
		checkInvalid();
		return locateFieldOrFail(fieldName).removeIndex();
	}
	
	@Override
	public boolean hasIndex(String fieldName) {
		checkInvalid();
		return locateFieldOrFail(fieldName).hasIndex();
	}
	
	@Override
	public boolean isIndexUnique(String fieldName) {
		checkInvalid();
		return locateFieldOrFail(fieldName).isIndexUnique();
	}
	
	private ZooField locateFieldOrFail(String fieldName) {
		ZooField f = getField(fieldName);
		if (f == null) {
			throw new IllegalArgumentException("Field not found: " + fieldName);
		}
		return f;
	}
	
	@Override
	public void dropInstances() {
		checkInvalid();
		schemaManager.dropInstances(this);
	}

	@Override
	public void rename(String newName) {
		checkInvalid();
		schemaManager.renameSchema(def, newName);
	}

	@Override
	public String getName() {
		checkInvalid();
		return def.getClassName();
	}

	@Override
	public ZooClass getSuperClass() {
		checkInvalid();
		if (def.getSuperDef() == null) {
			return null;
		}
		return def.getSuperDef().getVersionProxy();
	}

	@Override
	public List<ZooField> getAllFields() {
		checkInvalid();
		ArrayList<ZooField> ret = new ArrayList<ZooField>();
		for (ZooFieldDef fd: def.getAllFields()) {
			ret.add(fd.getProxy());
		}
		return ret;
	}

	@Override
	public List<ZooField> getLocalFields() {
		checkInvalid();
		ArrayList<ZooField> ret = new ArrayList<ZooField>();
		for (ZooFieldDef fd: def.getAllFields()) {
			if (fd.getDeclaringType() == def) {
				ret.add(fd.getProxy());
			}
		}
		return ret;
	}

	@Override
	public ZooField defineField(String fieldName, Class<?> type) {
		checkAddField(fieldName);
		ZooFieldDef field = schemaManager.addField(def, fieldName, type);
		//Update, in case it changed
		def = field.getDeclaringType();
		return field.getProxy();
	}

	@Override
	public ZooField defineField(String fieldName, ZooClass type, int arrayDepth) {
		checkAddField(fieldName);
		ZooClassDef typeDef = ((ZooClassProxy)type).getSchemaDef();
		ZooFieldDef field = schemaManager.addField(def, fieldName, typeDef, arrayDepth);
		//Update, in case it changed
		def = field.getDeclaringType();
		return field.getProxy();
	}
	
	private void checkAddField(String fieldName) {
		checkInvalid();
		checkJavaFieldNameConformity(fieldName);
		//check existing names
		for (ZooFieldDef fd: def.getAllFields()) {
			if (fd.getName().equals(fieldName)) {
				throw new IllegalArgumentException("Field name already defined: " + fieldName);
			}
		}
	}
	
	
	static void checkJavaFieldNameConformity(String fieldName) {
		if (fieldName == null || fieldName.length() == 0) {
			throw new IllegalArgumentException("Field name invalid: '" + fieldName + "'");
		}
		for (int i = 0; i < fieldName.length(); i++) {
			char c = fieldName.charAt(i);
			if (i == 0) {
				if (!Character.isJavaIdentifierStart(c)) {
					throw new IllegalArgumentException("Field name invalid: " + fieldName);
				}
			} else {
				if (!Character.isJavaIdentifierPart(c)) {
					throw new IllegalArgumentException("Field name invalid: " + fieldName);
				}
			}
		}
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
	public ZooField getField(String fieldName) {
		checkInvalid();
		for (ZooFieldDef f: def.getAllFields()) {
			if (f.getName().equals(fieldName)) {
				return f.getProxy();
			}
		}
		return null;
	}
	
	@Override
	public void removeField(String fieldName) {
		checkInvalid();
		ZooField f = getField(fieldName);
		if (f == null) {
			throw new IllegalStateException(
					"Field not found: " + def.getClassName() + "." + fieldName);
		}
		removeField(f);
	}

	@Override
	public void removeField(ZooField field) {
		checkInvalid();
		ZooFieldDef fieldDef = ((ZooFieldProxy)field).getFieldDef();
		def = schemaManager.removeField(fieldDef);
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
		for (ZooClassProxy sub: subClasses) {
			subs.add(sub);
		}
		return subs;
	}

	@Override
	public Iterator<?> getInstanceIterator() {
		checkInvalid();
		//TODO return CloseableIterator instead?
		return def.jdoZooGetNode().loadAllInstances(this, true);
	}

	@Override
	public Iterator<ZooHandle> getHandleIterator(boolean subClasses) {
		checkInvalid();
		//TODO return CloseableIterator instead?
		return new IteratorTypeAdapter<ZooHandle>(
				def.jdoZooGetNode().oidIterator(this, subClasses));
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

	public List<ZooClassProxy> getSubProxies() {
		return subClasses;
	}

	/**
	 * 
	 * @return Schema ID which is independent of the schema version.
	 */
	public long getSchemaId() {
		return schemaId;
	}

	/**
	 * Schema operation callback for removing this class definition.
	 */
	public void socRemoveDef() {
		superProxy.subClasses.remove(this);
	}
	
	/**
	 * Schema operation callback for removing this class definition.
	 */
	public void socRemoveDefRollback() {
		superProxy.subClasses.add(this);
	}

	@Override
	public long instanceCount(boolean subClasses) {
		checkInvalid();
		if (def.jdoZooIsNew() && def.getSchemaVersion() == 0) {
			return 0;
		}
		return def.jdoZooGetNode().countInstances(this, subClasses);
	}

	@Override
	public ZooHandle newInstance() {
		GenericObject go = GenericObject.newEmptyInstance(def, session.internalGetCache());
		ZooHandleImpl hdl = go.getOrCreateHandle();
		return hdl;
	}

	@Override
	public ZooHandle newInstance(long oid) {
		if (session.isOidUsed(oid)) {
			throw new IllegalArgumentException(
					"An object with this OID already exists: " + Util.oidToString(oid));
		}
		GenericObject go = GenericObject.newEmptyInstance(oid, def, session.internalGetCache());
		ZooHandleImpl hdl = go.getOrCreateHandle();
		return hdl;
	}
}
