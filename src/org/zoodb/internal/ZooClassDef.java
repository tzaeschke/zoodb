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
package org.zoodb.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.ZooFieldDef.JdoType;
import org.zoodb.internal.client.PCContext;
import org.zoodb.internal.client.SchemaOperation;
import org.zoodb.internal.client.session.ClientSessionCache;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Util;

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
 * @author Tilmann Zaeschke
 */
public class ZooClassDef extends ZooPCImpl {

	private String className;
	private transient Class<?> cls;
	
	private long oidSuper;
	//This is a unique Schema ID. All versions of a schema have the same ID.
	private final long schemaId;
	private final short versionId;
	private transient ZooClassDef superDef;
	private transient ZooClassProxy versionProxy = null;
	
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
		versionId = -1;
	}
	
	private ZooClassDef(String clsName, long oid, long superOid, long schemaId, int versionId) {
		jdoZooSetOid(oid);
		this.className = clsName;
		this.oidSuper = superOid;
		this.schemaId = schemaId;
		this.versionId = (short) versionId;
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
		ZooClassDef x = new ZooClassDef(ZooPCImpl.class.getName(), 50, 0, 50, 0);
		x.cls = ZooPCImpl.class;
		x.className = ZooPCImpl.class.getName();
		//x.associateFields(); //doesn't seem to be necessary
		return x;
	}
	
	/**
	 * Methods used for bootstrapping the schema of newly created databases.
	 * @return Meta schema instance
	 */
	public static ZooClassDef bootstrapZooClassDef() {
		ZooClassDef meta = new ZooClassDef(ZooClassDef.class.getName(), 51, 50, 51, 0);
		ArrayList<ZooFieldDef> fields = new ArrayList<ZooFieldDef>();
		fields.add(new ZooFieldDef(meta, "className", String.class.getName(), 0, 
				JdoType.STRING, 70));
		fields.add(new ZooFieldDef(meta, "oidSuper", long.class.getName(), 0, 
				JdoType.PRIMITIVE, 71));
        fields.add(new ZooFieldDef(meta, "schemaId", long.class.getName(), 0, 
        		JdoType.PRIMITIVE, 72));
        fields.add(new ZooFieldDef(meta, "versionId", short.class.getName(), 0, 
        		JdoType.PRIMITIVE, 73));
		fields.add(new ZooFieldDef(meta, "localFields", ArrayList.class.getName(), 0, 
				JdoType.SCO, 74));
		fields.add(new ZooFieldDef(meta, "prevVersionOid", long.class.getName(), 0, 
				JdoType.PRIMITIVE, 75));
		fields.add(new ZooFieldDef(meta, "evolutionOperations", ArrayList.class.getName(), 0, 
				JdoType.SCO, 76));
		//new ZooFieldDef(this, allFields, ZooFieldDef[].class.getName(), typeOid, JdoType.ARRAY);
		meta.registerFields(fields);
		meta.cls = ZooClassDef.class;
		meta.className = ZooClassDef.class.getName();

		meta.associateFields();
		meta.associateJavaTypes();
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
		for (ZooClassProxy sub: versionProxy.getSubProxies()) {
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
	private ZooClassDef newVersion(ClientSessionCache cache, List<SchemaOperation> ops, 
			ZooClassDef newSuper) {
		if (nextVersion != null) {
			throw new IllegalStateException();
		}

		if (newSuper == null) {
			//no new version of super available
			newSuper = superDef; 
		}
		
		long oid = jdoZooGetContext().getNode().getOidBuffer().allocateOid();
		ZooClassDef newDef = new ZooClassDef(className, oid, newSuper.getOid(), schemaId,
		        versionId + 1);

		//super-class
		newDef.associateSuperDef(newSuper);
		
		//caches
		cache.addSchema(newDef, false, jdoZooGetContext().getNode());
		
		//versions
		newDef.prevVersionOid = jdoZooGetOid();
		newDef.prevVersion = this;
		nextVersion = newDef;
		
		//API class
		newDef.versionProxy = versionProxy;
		versionProxy.newVersion(newDef);
		
		//context
		newDef.providedContext = 
			new PCContext(newDef, providedContext.getSession(), providedContext.getNode());
		
		//fields
		for (ZooFieldDef f: localFields) {
			ZooFieldDef fNew = new ZooFieldDef(f, newDef);
			newDef.localFields.add(fNew);
			if (fNew.getProxy() != null) {
				fNew.getProxy().updateVersion(fNew);
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

	public ZooClassDef newVersionRollback(ZooClassDef newDef, ClientSessionCache cache) {
		if (nextVersion != newDef) {
			throw new IllegalStateException();
		}
		if (newDef.nextVersion != null) {
			throw new IllegalStateException();
		}

		//caches
		newDef.jdoZooMarkDeleted();
		cache.updateSchema(this, newDef.getJavaClass(), this.getJavaClass());
		
		//versions
		nextVersion = null;
		
		//API class
		versionProxy = newDef.versionProxy;
		versionProxy.newVersionRollback(newDef);
		
		//fields
		for (ZooFieldDef f: localFields) {
			if (f.getProxy() != null) {
				f.getProxy().updateVersion(f);
			}
		}
		
		//sub-classes are initialised via ensureLatestSuper().
		
		return newDef;
	}

	public static ZooClassDef declare(String clsName, long oid, long superOid) {
		return new ZooClassDef(clsName, oid, superOid, oid, 0);
	}
	
	public static ZooClassDef createFromJavaType(Class<?> cls, ZooClassDef defSuper,
			Node node, Session session) {
        //create instance
        ZooClassDef def;
        long superOid = 0;
        if (cls != ZooPCImpl.class) {
            if (defSuper == null) {
                throw DBLogger.newUser("Super class is not persistent capable: " + cls);
            }
            superOid = defSuper.getOid();
            if (superOid == 0) {
                throw DBLogger.newFatal("No super class found: " + cls.getName());
            }
        }
        long oid = node.getOidBuffer().allocateOid();
        def = new ZooClassDef(cls.getName(), oid, superOid, oid, 0);

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
	        long fieldOid = node.getOidBuffer().allocateOid();
	        ZooFieldDef zField = ZooFieldDef.createFromJavaType(def, jField, fieldOid);
			fieldList.add(zField);
		}		

		// init class
		def.registerFields(fieldList);
		def.cls = cls;
		def.associateSuperDef(defSuper);
		def.associateProxy(new ZooClassProxy(def, session));
		def.associateFields();
		
		return def;
	}
	
	public void initProvidedContext(Session session, Node node) {
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
	
	void registerFields(List<ZooFieldDef> fieldList) {
        localFields.addAll(fieldList);
    }

    public void associateFCOs(Collection<ZooClassDef> cachedSchemata, 
    		boolean isSchemaAutoCreateMode, Set<String> missingSchemas) {
		//Fields:
		for (ZooFieldDef zField: localFields) {
			if (zField.isPrimitiveType()) {
				//no further work for primitives
				continue;
			}
			
			ZooClassDef typeDef = null;
			
			typeDef = zField.getType();
			if (typeDef != null) {
				//do we need to find the latest type? I think so..., if the type has been
				//renamed AND modified...
				while (typeDef.getNextVersion() != null) {
					typeDef = typeDef.getNextVersion();
				}
			}
			
			if (typeDef == null) {
				String typeName = zField.getTypeName();
				for (ZooClassDef cs: cachedSchemata) {
					if (cs.getClassName().equals(typeName)) {
						typeDef = cs;
						break;
					}
				}
			}
			
			if (typeDef == null) {
				if (zField.getJdoType() == JdoType.REFERENCE) {
					if (isSchemaAutoCreateMode) {
						missingSchemas.add(zField.getTypeName());
						continue;
					}
					String typeName = zField.getTypeName();
					throw DBLogger.newUser("Schema error, class " + getClassName() + " references "
							+ "class " + typeName + " as embedded object, but embedded objects "
							+ "of this type are not allowed. If it extend ZooPCImpl or "
							+ "PersistenceCapableImpl then it should have its own schema defined.");
				}
				//found SCO
				continue;
			}

			if (typeDef.jdoZooIsDeleted()) {
				throw DBLogger.newUser("Schema error, class " + getClassName() + " references "
						+ "class " + typeDef.getClassName() + ", but this has been deleted.");
			}
			
			//This is actually only necessary if the type is not yet assigned.
			//It may already be assigned in cases where the type has been renamed.
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
		associateJavaTypes(false);
	}

	public void associateJavaTypes(boolean failForMismatch) {
		if (cls != null) {
			if (!className.equals(ZooClassDef.class.getName()) && 
					!className.equals(ZooPCImpl.class.getName())) {
				System.out.println("This is new, FIX this!"); //TODO remove
				return;
				//throw new IllegalStateException(cls.getName());
			}
		}
		
		if (nextVersion != null) {
			//Java classes are unlikely to fit with outdated schemas
			return;
		}
		
		String fName = null;
		try {
			Class<?> tmpClass = Class.forName(className);
			for (ZooFieldDef f: localFields) {
				fName = f.getName();
				Field jf = tmpClass.getDeclaredField(fName);
				//this may fail due to incompatibilities!
				f.setJavaField(jf);
			}
			isJavaCompatible = true;
			cls = tmpClass;
		} catch (ClassNotFoundException e) {
			//okay we will use artifical/generic classes
		    return;
		} catch (SecurityException e) {
			throw DBLogger.newFatal("No access to class fields: " + className + "." + fName, e);
		} catch (NoSuchFieldException e) {
			//okay, Java class is incompatible. We continue anyway, but ensure that the
			//Java deserializer is not used.
			for (ZooFieldDef f: localFields) {
				f.unsetJavaField();
			}
			return;
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
			cls = null;
			if (failForMismatch) {
				throw DBLogger.newUser("Schema error, field count mismatch between Java class (" +
					n + ") and database class (" + localFields.size() + ").");
			}
		}
	}

	public ArrayList<ZooFieldDef> getLocalFields() {
		return localFields;
	}
	
	public ZooFieldDef[] getAllFields() {
		return allFields;
	}

	public ZooClassProxy getVersionProxy() {
		return versionProxy;
	}
	
	public long getSuperOID() {
		return oidSuper;
	}

	public ZooClassDef getSuperDef() {
		return superDef;
	}

	public void associateVersions(Map<Long, ZooClassDef> schemata) {
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
		int i = allFields.size();
		for (ZooFieldDef f: localFields) {
			f.setOffset(ofs, i);
			ofs = f.getNextOffset();
			allFields.add(f);
			i++;
		}
		
		this.allFields = allFields.toArray(new ZooFieldDef[allFields.size()]);
	}

	public ZooFieldDef getField(String attrName) {
		for (ZooFieldDef f: allFields) {
			if (f.getName().equals(attrName)) {
				return f;
			}
		}
		throw DBLogger.newUser("Field name not found: " + attrName);
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
    
	public void addField(ZooFieldDef field) {
		localFields.add(field);
		rebuildFieldsRecursive();
		newEvolutionOperationAdd(allFields.length-1, null);
	}
	
	void rebuildFieldsRecursive() {
		associateFields();
		for (ZooClassProxy c: versionProxy.getSubProxies()) {
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
		for (ZooClassProxy sub: versionProxy.getSubProxies()) {
			sub.getSchemaDef().newEvolutionOperationAdd(fieldId, initialValue);
		}
	}
	
	private void newEvolutionOperationRemove(int fieldId) {
		newEvolutionOperation(PersistentSchemaOperation.newRemoveOperation(fieldId));
		for (ZooClassProxy sub: versionProxy.getSubProxies()) {
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

    /**
     * Returns the version number of this schema version, starting with 0.
     */
    public int getSchemaVersion() {
        return versionId;
    }

	public void associateProxy(ZooClassProxy px) {
		if (versionProxy != null) {
			throw new IllegalStateException();
		}
		versionProxy = px;
	}

	/**
	 * 
	 * @param def
	 * @return True if this class is the same or a super-type of 'def'. Otherwise returns false.
	 */
	public boolean isSuperTypeOf(ZooClassDef def) {
		if (this == def) {
			return true;
		}
		if (def.superDef == null) {
			return false;
		}
		return isSuperTypeOf(def.superDef);
	}
}
