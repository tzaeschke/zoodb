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
package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.ZooFieldDef.JdoType;
import org.zoodb.jdo.internal.client.PCContext;
import org.zoodb.jdo.internal.client.SchemaOperation;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.internal.model1p.Node1P;
import org.zoodb.jdo.internal.util.Util;

/**
 * ZooClassDef represents a class schema definition used by the database. 
 * The highest stored schema is that of PersistenceCapableImpl.
 * 
 * Initialization takes three(four) steps:
 * 1) Instance creation and association with super OID.
 * 2) Association with super class (this may not be instantiated during 1).
 * 3) Initialization of fields and their offsets. This requires a complete inheritance hierarchy.
 * 4) Update FCO fields with ClassDef OIDs.
 * 
 * @author Tilmann Zäschke
 */
public class ZooClassDef extends ZooPCImpl {

	private String className;
	private transient Class<?> cls;
	
	private final long oidSuper;
	//This is a unique Schema ID. All versions of a schema have the same ID.
	private final long schemaId;
	private transient ZooClassDef superDef;
	private transient SchemaClassProxy apiHandle = null;
	
	private final ArrayList<ZooFieldDef> localFields = new ArrayList<ZooFieldDef>(10);
	private transient ZooFieldDef[] allFields = new ZooFieldDef[0];
	private transient HashMap<String, ZooFieldDef> fieldBuffer = null;
	private transient PCContext providedContext = null;
	
	private long prevVersionOid = 0;
	private transient ZooClassDef nextVersion = null;
	private transient ZooClassDef prevVersion = null;
	//Indicates whether the class is schema-compatible with the Java class of the same name
	private transient boolean isJavaCompatible = false;  
	
	//List of operations that transform a previous version into the current version. 
	private ArrayList<PersistentSchemaOperation> evolutionOperations = null;
	
	private ZooClassDef() {
		//DO not use, for de-serializer only!
		oidSuper = 0;
		schemaId = 0;
	}
	
	private ZooClassDef(String clsName, long oid, long superOid, long schemaId) {
		jdoZooSetOid(oid);
		this.className = clsName;
		this.oidSuper = superOid;
		this.schemaId = schemaId;
	}
	
	/**
	 * Methods used for bootstrapping the schema of newly created databases.
	 * @return Root schema
	 */
	public static ZooClassDef bootstrapZooPCImpl() {
		//The bootstrapped schemata have a fixed OID.
		//This is because they also need to be created every time we open a database.
		//They are required to actually read the boot-schema from the database ....
		//Actually, we don't really need to read them, but they shold be in the database
		//anyway, for consistency.
		//TODO maybe we don't need to store ZooClassDef????
		// -> and ZooPC does not need to be bootstrapped in memory????
		ZooClassDef x = new ZooClassDef(ZooPCImpl.class.getName(), 50, 0, 50);
		x.cls = ZooPCImpl.class;
		x.className = ZooPCImpl.class.getName();
		return x;
	}
	
	/**
	 * Methods used for bootstrapping the schema of newly created databases.
	 * @return Meta schema instance
	 */
	public static ZooClassDef bootstrapZooClassDef() {
		ZooClassDef meta = new ZooClassDef(ZooClassDef.class.getName(), 51, 50, 51);
		ArrayList<ZooFieldDef> fields = new ArrayList<ZooFieldDef>();
		fields.add(new ZooFieldDef(meta, "className", String.class.getName(), JdoType.STRING));
		fields.add(new ZooFieldDef(meta, "oidSuper", long.class.getName(), JdoType.PRIMITIVE));
		fields.add(new ZooFieldDef(meta, "localFields", ArrayList.class.getName(), JdoType.SCO));
		fields.add(new ZooFieldDef(meta, "prevVersionOid", long.class.getName(), JdoType.PRIMITIVE));
		fields.add(new ZooFieldDef(meta, "evolutionOperations", ArrayList.class.getName(), JdoType.SCO));
		//new ZooFieldDef(this, allFields, ZooFieldDef[].class.getName(), typeOid, JdoType.ARRAY);
		meta.regisaterFields(fields);
		meta.cls = ZooClassDef.class;
		meta.className = ZooClassDef.class.getName();
		return meta;
	}
	

	public ZooClassDef getModifiableVersion(ClientSessionCache cache, List<SchemaOperation> ops) {
		return getModifiableVersion(cache, ops, null);		
	}
	
	public ZooClassDef getModifiableVersion(ClientSessionCache cache, List<SchemaOperation> ops, 
			ZooClassDef newSuper) {
		if (this.jdoZooIsNew()) {
			//this happens for example when the super-class is modified AFTER the local class got 
			//a new version.
			//In other words:
			//First we add all sub-classes. This is important, because getModifiableVersion() only
			//add a new version to the super-class if it actually creates a new version. If there
			//is already a modifiable sub-class, it would not be added.
			ensureLatestSuper();
			return this;
		} 
		
		ZooClassDef defNew = newVersion(cache, ops, newSuper);
		ops.add(new SchemaOperation.SchemaNewVersion(this, defNew, cache));
		for (SchemaClassProxy sub: apiHandle.getSubProxies()) {
			//ensure that all sub-classes become modifiable versions.
			sub.getSchemaDef().getModifiableVersion(cache, ops, defNew);
		}
		
		return defNew;
	}

	/**
	 * Schema versioning: We only create new schema instance when we add or remove fields.
	 * Renaming a field should not result in a new version!
	 * A new version is only required when the modified schema does not match the stored data. Such
	 * changes require also new versions of all sub-classes. 
	 * WHY? If every class stored only their own fields would we still have a problem? Yes,
	 * because the new version of the referenced superclass has a different OID.
	 * @param cache 
	 * 
	 * @return New version.
	 */
	public ZooClassDef newVersion(ClientSessionCache cache, List<SchemaOperation> ops, 
			ZooClassDef newSuper) {
		if (nextVersion != null) {
			throw new IllegalStateException();
		}

		if (newSuper == null) {
			//no new version of super available
			newSuper = superDef; 
		}
		
		long oid = jdoZooGetContext().getNode().getOidBuffer().allocateOid();
		ZooClassDef newDef = new ZooClassDef(className, oid, newSuper.getOid(), schemaId);

		//caches
		cache.addSchema(newDef, false, jdoZooGetContext().getNode());
		
		//versions
		newDef.prevVersionOid = jdoZooGetOid();
		newDef.prevVersion = this;
		nextVersion = newDef;
		
		//API class
		newDef.apiHandle = apiHandle;
		apiHandle.newVersion(newDef);
		
		//context
		newDef.providedContext = 
			new PCContext(newDef, providedContext.getSession(), providedContext.getNode());
		
		//fields
		for (ZooFieldDef f: localFields) {
			ZooFieldDef fNew = new ZooFieldDef(f, newDef);
			newDef.localFields.add(fNew);
			if (fNew.getApiHandle() != null) {
				fNew.getApiHandle().updateVersion(fNew);
			}
		}
		newDef.associateFields();
		
		return newDef;
	}

	public void ensureLatestSuper() {
		//This is also the general population function for the sub-class list
		if (superDef.getNextVersion() != null) {
			superDef = superDef.getNextVersion();
			//This is the only place where we may change the super-oid, because the local class is
			//modifiable and the super-class becomes modifiable as well.
			oidSuper = superDef.getOid();
		}
		if (superDef.getNextVersion() != null) {
			throw new IllegalStateException();
		}
	}

	public ZooClassDef newVersionRollback(ZooClassDef newDef, ClientSessionCache cache, 
			boolean superDefRollBack) {
		if (nextVersion != newDef) {
			throw new IllegalStateException();
		}
		if (newDef.nextVersion != null) {
			throw new IllegalStateException();
		}
		//super-class (update only because it is transient!)
		if (!superDefRollBack) {
			//the superDef does not have a new version, so we update it.
			superDef.removeSubClass(newDef);
			superDef.addSubClass(this);
		}

		//caches
		newDef.jdoZooMarkDeleted();
		cache.updateSchema(this, newDef.getJavaClass(), this.getJavaClass());
		
		//versions
		nextVersion = null;
		
		//API class
		apiHandle = newDef.apiHandle;
		apiHandle.newVersionRollback(this);
		
		//fields
		for (ZooFieldDef f: localFields) {
			if (f.getApiHandle() != null) {
				f.getApiHandle().updateVersion(f);
			}
		}
		
		//sub-classes are initialised via ensureLatestSuper().
		
		return newDef;
	}

	public static ZooClassDef createFromDatabase(String clsName, long oid, long superOid) {
		return new ZooClassDef(clsName, oid, superOid, oid);
	}
	
	public static ZooClassDef createFromJavaType(Class<?> cls, long oid, ZooClassDef defSuper,
			Node node, Session session) {
        //create instance
        ZooClassDef def;
        long superOid = 0;
        if (cls != ZooPCImpl.class) {
            if (defSuper == null) {
                throw new JDOUserException("Super class is not persistent capable: " + cls);
            }
            superOid = defSuper.getOid();
            if (superOid == 0) {
                throw new IllegalStateException("No super class found: " + cls.getName());
            }
        }
        def = new ZooClassDef(cls.getName(), oid, superOid, oid);

        //local fields:
		ArrayList<ZooFieldDef> fieldList = new ArrayList<ZooFieldDef>();
		Field[] fields = cls.getDeclaredFields();
		for (int i = 0; i < fields.length; i++) {
			Field jField = fields[i];
			if (Modifier.isStatic(jField.getModifiers()) || 
					Modifier.isTransient(jField.getModifiers())) {
				continue;
			}
			//we cannot set references to other ZooClassDefs yet, as they may not be made 
			//persistent yet
			ZooFieldDef zField = ZooFieldDef.createFromJavaType(def, jField);
			fieldList.add(zField);
		}		

		// init class
		def.regisaterFields(fieldList);
		def.cls = cls;
		def.associateSuperDef(defSuper);
		def.associateFields();
		
		return def;
	}
	
	public void initProvidedContext(ObjectState state, Session session, Node node) {
		if (providedContext != null) {
			if (!className.equals(ZooClassDef.class.getName())) {
				throw new IllegalStateException(className);
			} else {
				//ignore resetting attempt for ZooClassDef
				return;
			}
		}
		providedContext =  new PCContext(this, session, node);
	}
	
	/**
	 * 
	 * @return The context that this classDef provides for instances of its class.
	 */
	public final PCContext getProvidedContext() {
		return providedContext;
	}
	
	void regisaterFields(List<ZooFieldDef> fieldList) {
        localFields.addAll(fieldList);
    }

    public void associateFCOs(Node1P node, Collection<ZooClassDef> cachedSchemata) {
		//Fields:
		for (ZooFieldDef zField: localFields) {
			String typeName = zField.getTypeName();
			
			if (zField.isPrimitiveType()) {
				//no further work for primitives
				continue;
			}
			
			ZooClassDef typeDef = null;
			
			for (ZooClassDef cs: cachedSchemata) {
				if (cs.getClassName().equals(typeName)) {
					typeDef = cs;
					break;
				}
			}
			
			if (typeDef==null) {
				//found SCO
				continue;
			}
			
			zField.setType(typeDef);
		}
	}
	
	public String getClassName() {
		return className;
	}

	public long getOid() {
		return jdoZooGetOid();
	}
	
	public Class<?> getJavaClass() {
		return cls;
	}

	public void associateJavaTypes() {
		if (cls != null) {
			if (!className.equals(ZooClassDef.class.getName()) && 
					!className.equals(ZooPCImpl.class.getName())) {	
				throw new IllegalStateException();
			}
		}
		
		if (nextVersion != null) {
			//Java classes are unlikely to fit with outdated schemas
			return;
		}
		
		String fName = null;
		try {
			cls = Class.forName(className);
			for (ZooFieldDef f: localFields) {
				fName = f.getName();
				Field jf = cls.getDeclaredField(fName);
				f.setJavaField(jf);
			}
			isJavaCompatible = true;
		} catch (ClassNotFoundException e) {
		    System.err.println("Class not found: " + className);
		    //cls = ClassCreator.createClass(className, superDef.getClassName());
		    return;
		} catch (SecurityException e) {
			throw new JDOFatalDataStoreException("No access to class fields: " + className + "." +
					fName, e);
		} catch (NoSuchFieldException e) {
			//okay, Java class is incompatible. We continue anyway, but ensure that the
			//Java deserializer is not used.
			for (ZooFieldDef f: localFields) {
				f.unsetJavaField();
			}
			return;
			//throw new JDOUserException("Schema error, field not found in Java class: " + 
			//		className + "." + fName, e);
		}

		// We check field mismatches and missing Java fields above. 
		// Now check field count, this should cover missing schema fields (too many Java fields).
		// we need to filter out transient and static fields
		int n = 0;
		for (Field f: cls.getDeclaredFields()) {
			int mod = f.getModifiers();
			if (Modifier.isTransient(mod) || Modifier.isStatic(mod)) {
				continue;
			}
			n++;
		}
		if (localFields.size() != n) {
			throw new JDOUserException("Schema error, field count mismatch between Java class (" +
					n + ") and database class (" + localFields.size() + ").");
		}
	}

	public ArrayList<ZooFieldDef> getLocalFields() {
		return localFields;
	}
	
	public ZooFieldDef[] getAllFields() {
		return allFields;
	}

	public SchemaClassProxy getApiHandle() {
		return apiHandle;
	}
	
	public long getSuperOID() {
		return oidSuper;
	}

	public ZooClassDef getSuperDef() {
		return superDef;
	}

	public void associateVersions(Map<Long,ZooClassDef> schemata) {
		if (prevVersionOid != 0) {
			prevVersion = schemata.get(prevVersionOid);
			prevVersion.nextVersion = this;
		}
	}
	
	/**
	 * Only to be used during database startup to load the schema-tree.
	 * @param superDef
	 */
	public void associateSuperDef(ZooClassDef superDef) {
		if (this.superDef != null) {
			throw new IllegalStateException();
		}

		if (superDef == null) {
			throw new IllegalArgumentException();
		}
		
		//class invariant
		if (superDef.getOid() != oidSuper) {
			throw new IllegalStateException("s-oid= " + oidSuper + " / " + superDef.getOid() + 
					"  class=" + className);
		}
		
		//check previous version...
		apiHandle = new SchemaClassProxy(this, jdoZooGetContext().getNode(), 
				jdoZooGetContext().getSession().getSchemaManager());
sss
		if (getNextVersion() == null) {
			superDef.addSubClass(this);
		}
		this.superDef = superDef;
	}

	public void associateFields() {
		ArrayList<ZooFieldDef> allFields = new ArrayList<ZooFieldDef>();
		
		//For PersistenceCapableImpl _super may be null:
		ZooClassDef sup = superDef;
		while (sup != null) {
			allFields.addAll(0, sup.getLocalFields());
			sup = sup.superDef;
		}

		int ofs = ZooFieldDef.OFS_INIITIAL; //8 + 8; //Schema-OID + OID
		if (!allFields.isEmpty()) {
			ofs = allFields.get(allFields.size()-1).getNextOffset();
		}

		//local fields:
		for (ZooFieldDef f: localFields) {
			f.setOffset(ofs);
			ofs = f.getNextOffset();
			allFields.add(f);
		}
		
		this.allFields = allFields.toArray(new ZooFieldDef[allFields.size()]);
	}

	public ZooFieldDef getField(String attrName) {
		for (ZooFieldDef f: allFields) {
			if (f.getName().equals(attrName)) {
				return f;
			}
		}
		throw new JDOUserException("Field name not found: " + attrName);
	}

	private void addSubClass(ZooClassDef sub) {
		subs.add(sub);
	}
	
	public void removeSubClass(ZooClassDef sub) {
		if (!subs.remove(sub)) {
			throw new IllegalArgumentException();
		}
	}
	
	public Map<String, ZooFieldDef> getAllFieldsAsMap() {
		if (fieldBuffer == null) {
			fieldBuffer = new HashMap<String, ZooFieldDef>();
			for (ZooFieldDef def: getAllFields()) {
				fieldBuffer.put(def.getName(), def);
			}
		}
		return fieldBuffer;
	}

	public boolean hasSuperClass(ZooClassDef cls) {
		if (superDef == cls) {
			return true;
		}
		if (superDef == null) {
			return false;
		}
		return superDef.hasSuperClass(cls);
	}
	
	@Override
	public String toString() {
		return className + " (" + Util.oidToString(getOid()) + ") super=" + superDef; 
	}

	public void rename(String newName) {
		zooActivateWrite();
		className = newName;
		cls = null;
		fieldBuffer = null;
		associateJavaTypes();
	}
    
    public void removeDef() {
        if (superDef != null) {
            superDef.subs.remove(this);
        }
    }
    
    public void removeDefRollback() {
        if (superDef != null) {
            superDef.addSubClass(this);
        }
    }

	public void addField(ZooFieldDef field) {
		localFields.add(field);
		rebuildFieldsRecursive();
		newEvolutionOperationAdd(allFields.length-1, null);
	}
	
	void rebuildFieldsRecursive() {
		associateFields();
		for (SchemaClassProxy c: getSubClassesLatestVersions()) {
			c.getSchemaDef().rebuildFieldsRecursive();
		}
	}
	
	public void removeField(ZooFieldDef fieldDef) {
		int i = 0;
		// remove from localFields
		for (ZooFieldDef fd: localFields) {
			if (fd.getName().equals(fieldDef.getName())) {
				localFields.remove(i);
				break;
			}
			i++;
		}
		// for op, use position in allFields
		i = 0;
		for (ZooFieldDef fd: allFields) {
			if (fd.getName().equals(fieldDef.getName())) {
				rebuildFieldsRecursive();
				newEvolutionOperationRemove(i);
				return;
			}
			i++;
		}
		throw new IllegalStateException("Field not found: " + fieldDef);
	}

	public ZooClassDef getNextVersion() {
		return nextVersion;
	}

	public ZooClassDef getPreviousVersion() {
		return prevVersion;
	}
	
	private void newEvolutionOperation(PersistentSchemaOperation op) {
		if (evolutionOperations == null) {
			evolutionOperations = new ArrayList<PersistentSchemaOperation>();
		}
		evolutionOperations.add(op);
	}
	
	private void newEvolutionOperationAdd(int fieldId, Object initialValue) {
		newEvolutionOperation(PersistentSchemaOperation.newAddOperation(
				fieldId, allFields[fieldId], initialValue));
		for (SchemaClassProxy sub: subs) {
			sub.getSchemaDef().newEvolutionOperationAdd(fieldId, initialValue);
		}
	}
	
	private void newEvolutionOperationRemove(int fieldId) {
		newEvolutionOperation(PersistentSchemaOperation.newRemoveOperation(fieldId));
		for (SchemaClassProxy sub: subs) {
			sub.getSchemaDef().newEvolutionOperationRemove(fieldId);
		}
	}
	
	/**
	 * The List of evolution operations contains all operations that are required to turn a 
	 * previous schema version into the present schema version. This includes also operations
	 * on super-classes. Field-IDs are relative to the allFields[].
	 * 
	 * @return List of operations
	 */
	public List<PersistentSchemaOperation> getEvolutionOps() {
		return evolutionOperations;
	}

	/**
	 * Returns the unique schema ID which is independent of the schema version.
	 */
	public long getSchemaId() {
		return schemaId;
	}
}
