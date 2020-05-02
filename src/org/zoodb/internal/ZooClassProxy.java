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
package org.zoodb.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.client.SchemaManager;
import org.zoodb.internal.util.ClassCreator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.DBTracer;
import org.zoodb.internal.util.IteratorTypeAdapter;
import org.zoodb.internal.util.Util;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooField;
import org.zoodb.schema.ZooHandle;

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
	private final SchemaManager schemaManager;
	private final long schemaId;
	private ArrayList<ZooClassProxy> subClasses = new ArrayList<ZooClassProxy>();
	private final Session session;
	private boolean isValid = true;
	
	public ZooClassProxy(ZooClassDef def, Session session) {
		this.def = def;
		this.schemaManager = session.getSchemaManager();
		this.schemaId = def.getSchemaId();
		this.session = session;
		ZooClassDef defSuper = def.getSuperDef();
		if (!def.getClassName().equals(ZooPC.class.getName())) {
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
		DBTracer.logCall(this);
		checkInvalidWrite();
		schemaManager.deleteSchema(this, false);
	}

	@Override
	public void removeWithSubClasses() {
		DBTracer.logCall(this);
		checkInvalidWrite();
		schemaManager.deleteSchema(this, true);
	}

	public ZooClassDef getSchemaDef() {
		DBTracer.logCall(this);
		checkInvalidRead();
		return def;
	}
	
	protected void checkInvalidWrite() {
		checkInvalid(true);
	}
	
	protected void checkInvalidRead() {
		checkInvalid(false);
	}
	
	protected void checkInvalid(boolean write) {
		if (!isValid) {
			throw new IllegalStateException("This schema has been invalidated.");
		}
		if (session.isClosed()) {
			throw new IllegalStateException("This schema belongs to a closed PersistenceManager.");
		}
		//TODO For READ we do NOT check for an active transaction here. Reasons:
		//1) All schemata should be cached in memory. We only need an active transaction
		//   for refreshing schemata.
		//2) JDO has many operations that do not require an active transaction, such as creating
		//   a new Query. Conceptually, these belong to the PM, which has no 'active' state.
		//BUT: For now we just fail. We would need to distinguish weather we need schema-read or
		//     object-read (getHandleIterator())
		if (!session.isActive()) {
			if (write || !session.getConfig().getNonTransactionalRead()) {
				throw new IllegalStateException("The transaction is currently not active.");
			}
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
		DBTracer.logCall(this, fieldName, isUnique);
		checkInvalidWrite();
		locateFieldOrFail(fieldName).createIndex(isUnique);
	}
	
	@Override
	public boolean removeIndex(String fieldName) {
		DBTracer.logCall(this, fieldName);
		checkInvalidWrite();
		return locateFieldOrFail(fieldName).removeIndex();
	}
	
	@Override
	public boolean hasIndex(String fieldName) {
		DBTracer.logCall(this, fieldName);
		checkInvalidRead();
		return locateFieldOrFail(fieldName).hasIndex();
	}
	
	@Override
	public boolean isIndexUnique(String fieldName) {
		DBTracer.logCall(this, fieldName);
		checkInvalidRead();
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
		DBTracer.logCall(this);
		checkInvalidWrite();
		schemaManager.dropInstances(this);
	}

	@Override
	public void rename(String newName) {
		DBTracer.logCall(this, newName);
		checkInvalidWrite();
		schemaManager.renameSchema(def, newName);
	}

	@Override
	public String getName() {
		DBTracer.logCall(this);
		checkInvalidRead();
		return def.getClassName();
	}

	@Override
	public ZooClass getSuperClass() {
		DBTracer.logCall(this);
		checkInvalidRead();
		if (def.getSuperDef() == null) {
			return null;
		}
		return def.getSuperDef().getVersionProxy();
	}

	@Override
	public List<ZooField> getAllFields() {
		DBTracer.logCall(this);
		checkInvalidRead();
		ArrayList<ZooField> ret = new ArrayList<ZooField>();
		for (ZooFieldDef fd: def.getAllFields()) {
			ret.add(fd.getProxy());
		}
		return ret;
	}

	@Override
	public List<ZooField> getLocalFields() {
		DBTracer.logCall(this);
		checkInvalidRead();
		ArrayList<ZooField> ret = new ArrayList<ZooField>();
		for (ZooFieldDef fd: def.getAllFields()) {
			if (fd.getDeclaringType() == def) {
				ret.add(fd.getProxy());
			}
		}
		return ret;
	}

	@Override
	public ZooField addField(String fieldName, Class<?> type) {
		DBTracer.logCall(this, fieldName, type);
		checkAddField(fieldName);
		ZooFieldDef field = schemaManager.addField(def, fieldName, type);
		//Update, in case it changed
		def = field.getDeclaringType();
		return field.getProxy();
	}

	@Override
	public ZooField addField(String fieldName, ZooClass type, int arrayDepth) {
		DBTracer.logCall(this, fieldName, type, arrayDepth);
		checkAddField(fieldName);
		ZooClassDef typeDef = ((ZooClassProxy)type).getSchemaDef();
		ZooFieldDef field = schemaManager.addField(def, fieldName, typeDef, arrayDepth);
		//Update, in case it changed
		def = field.getDeclaringType();
		return field.getProxy();
	}
	
	private void checkAddField(String fieldName) {
		checkInvalidWrite();
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
		checkInvalidRead();
		return "Class schema(" + Util.getOidAsString(def) + "): " + def.getClassName();
	}

	@Override
	public Class<?> getJavaClass() {
		DBTracer.logCall(this);
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
		DBTracer.logCall(this, fieldName);
		checkInvalidRead();
		for (ZooFieldDef f: def.getAllFields()) {
			if (f.getName().equals(fieldName)) {
				return f.getProxy();
			}
		}
		return null;
	}
	
	@Override
	public void removeField(String fieldName) {
		DBTracer.logCall(this, fieldName);
		checkInvalidWrite();
		ZooField f = getField(fieldName);
		if (f == null) {
			throw new IllegalStateException(
					"Field not found: " + def.getClassName() + "." + fieldName);
		}
		removeField(f);
	}

	@Override
	public void removeField(ZooField field) {
		DBTracer.logCall(this, field);
		checkInvalidWrite();
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
		DBTracer.logCall(this);
		checkInvalidRead();
		ArrayList<ZooClass> subs = new ArrayList<ZooClass>();
		for (ZooClassProxy sub: subClasses) {
			subs.add(sub);
		}
		return subs;
	}

	@Override
	public Iterator<?> getInstanceIterator() {
		DBTracer.logCall(this);
		checkInvalidRead();
		return def.jdoZooGetNode().loadAllInstances(this, true);
	}

	@Override
	public Iterator<ZooHandle> getHandleIterator(boolean subClasses) {
		DBTracer.logCall(this, subClasses);
		checkInvalidRead();
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
		if (!superProxy.subClasses.remove(this)) {
			throw DBLogger.newFatalInternal("Schema structure is inconsistent.");
		}
	}
	
	/**
	 * Schema operation callback for removing this class definition.
	 */
	public void socRemoveDefRollback() {
		superProxy.subClasses.add(this);
	}

	@Override
	public long instanceCount(boolean subClasses) {
		DBTracer.logCall(this, subClasses);
		checkInvalidRead();
		if (def.jdoZooIsNew() && def.getSchemaVersion() == 0) {
			return 0;
		}
		return def.jdoZooGetNode().countInstances(this, subClasses);
	}

	@Override
	public ZooHandle newInstance() {
		DBTracer.logCall(this);
		GenericObject go = GenericObject.newEmptyInstance(def, session.internalGetCache());
		return go.getOrCreateHandle();
	}

	@Override
	public ZooHandle newInstance(long oid) {
		DBTracer.logCall(this, oid);
		if (session.isOidUsed(oid)) {
			throw new IllegalArgumentException(
					"An object with this OID already exists: " + Util.oidToString(oid));
		}
		GenericObject go = GenericObject.newEmptyInstance(oid, def, session.internalGetCache());
		return go.getOrCreateHandle();
	}
	
	public void invalidate() {
		isValid = false;
		//in case the fields were create through a Java class (in stead of schema operations)
		//we need to invalidate them from here
		for (ZooFieldDef f: def.getLocalFields()) {
			f.getProxy().invalidate();
		}
	}
}
