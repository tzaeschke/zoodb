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
import java.util.Arrays;
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
	private transient ZooClassDef superDef;
	private transient List<ZooClassDef> subs = new ArrayList<ZooClassDef>();
	private transient ISchema apiHandle = null;
	
	private final ArrayList<ZooFieldDef> localFields = new ArrayList<ZooFieldDef>(10);
	private transient ZooFieldDef[] allFields = new ZooFieldDef[0];
	private transient Map<String, ZooFieldDef> fieldBuffer = null;
	private transient PCContext providedContext = null;
	
	private long prevVersionOid = 0;
	private transient ZooClassDef nextVersion = null;
	private transient ZooClassDef prevVersion = null;
	
	private ZooClassDef() {
		//DO not use, for de-serializer only!
		oidSuper = 0;
	}
	
	private ZooClassDef(String clsName, long oid, long superOid) {
		jdoZooSetOid(oid);
		this.className = clsName;
		this.oidSuper = superOid;
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
		ZooClassDef x = new ZooClassDef(ZooPCImpl.class.getName(), 50, 0);
		x.cls = ZooPCImpl.class;
		x.className = ZooPCImpl.class.getName();
		return x;
	}
	
	/**
	 * Methods used for bootstrapping the schema of newly created databases.
	 * @return Meta schema instance
	 */
	public static ZooClassDef bootstrapZooClassDef() {
		ZooClassDef meta = new ZooClassDef(ZooClassDef.class.getName(), 51, 50);
		ArrayList<ZooFieldDef> fields = new ArrayList<ZooFieldDef>();
		fields.add(new ZooFieldDef(meta, "className", String.class.getName(), JdoType.STRING));
		fields.add(new ZooFieldDef(meta, "oidSuper", long.class.getName(), JdoType.PRIMITIVE));
		fields.add(new ZooFieldDef(meta, "localFields", ArrayList.class.getName(), JdoType.SCO));
		fields.add(new ZooFieldDef(meta, "prevVersionOid", long.class.getName(), JdoType.PRIMITIVE));
		//new ZooFieldDef(this, allFields, ZooFieldDef[].class.getName(), typeOid, JdoType.ARRAY);
		meta.regisaterFields(fields);
		meta.cls = ZooClassDef.class;
		meta.className = ZooClassDef.class.getName();
		return meta;
	}
	
	public ZooClassDef newVersion() {
		if (nextVersion != null) {
			throw new IllegalStateException();
		}
		//TODO also create new versions of subs?!?!? At least when adding attributes...
		//TODO update caches with new version
		long oid = jdoZooGetContext().getNode().getOidBuffer().allocateOid();
		ZooClassDef newDef = new ZooClassDef(className, oid, oidSuper);
		newDef.associateSuperDef(superDef);
		
		//versions
		newDef.prevVersionOid = jdoZooGetOid();
		newDef.prevVersion = this;
		nextVersion = newDef;
		
		//API class
		newDef.apiHandle = apiHandle;
		apiHandle = null;
		
		//context
		newDef.providedContext = 
			new PCContext(newDef, providedContext.getSession(), providedContext.getNode());
		
		//fields
		newDef.subs.addAll(subs);
		for (ZooFieldDef f: localFields) {
			ZooFieldDef fNew = 
				new ZooFieldDef(newDef, f.getName(), f.getTypeName(), f.getJdoType());
			newDef.localFields.add(fNew);
		}
		newDef.associateFields();
		
		//caches
		providedContext.getSession().makePersistent(newDef);
		
		return newDef;
	}

	public static ZooClassDef createFromDatabase(String clsName, long oid, long superOid) {
		return new ZooClassDef(clsName, oid, superOid);
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
        def = new ZooClassDef(cls.getName(), oid, superOid);

        //local fields:
		ArrayList<ZooFieldDef> fieldList = new ArrayList<ZooFieldDef>();
		Field[] fields = cls.getDeclaredFields();
		for (int i = 0; i < fields.length; i++) {
			Field jField = fields[i];
			if (Modifier.isStatic(jField.getModifiers()) || 
					Modifier.isTransient(jField.getModifiers())) {
				continue;
			}
			//we cannot set references to other ZooClassDefs yet, as they may not be made persistent 
			//yet
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
		
		String fName = null;
		try {
			cls = Class.forName(className);
			for (ZooFieldDef f: localFields) {
				fName = f.getName();
				Field jf = cls.getDeclaredField(fName);
				f.setJavaField(jf);
			}
		} catch (ClassNotFoundException e) {
		    //TODO this in only for checkDB ...
			//But what for? Possibly to allow checking of DB if stored classes are not in 
			//classpath...
		    System.err.println("Class not found: " + className);
		    //cls = ClassCreator.createClass(className, superDef.getClassName());
		    return;
			//throw new JDOFatalDataStoreException("Class not found: " + _className, e);
		} catch (SecurityException e) {
			throw new JDOFatalDataStoreException("No access to class fields: " + className + "." +
					fName, e);
		} catch (NoSuchFieldException e) {
			throw new JDOUserException("Schema error, field not found in Java class: " + 
					className + "." + fName, e);
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

	public ISchema getApiHandle() {
		if (apiHandle == null) {
			apiHandle = new ISchema(this, cls, jdoZooGetContext().getNode(), 
					jdoZooGetContext().getSession().getSchemaManager());
		}
		return apiHandle;
	}
	
	public long getSuperOID() {
		return oidSuper;
	}

	public ZooClassDef getSuperDef() {
		return superDef;
	}

	/**
	 * Only to be used during database startup to load the schema-tree.
	 * @param superDef
	 */
	public void associateSuperDef(ZooClassDef superDef) {
		if (this.superDef != null) {
			throw new IllegalStateException();
		}

		//For PersistenceCapableImpl this may be null:
		if (superDef != null) {
			//class invariant
			if (superDef.getOid() != oidSuper) {
				throw new IllegalStateException("s-oid= " + oidSuper + " / " + superDef.getOid() + 
						"  class=" + className);
			}
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
	
	public List<ZooClassDef> getSubClasses() {
		return subs;
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
		return className + " oid=" + Util.oidToString(getOid()) + " super=" + super.toString(); 
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
            superDef.subs.add(this);
        }
    }

	public ZooFieldDef addField(String fieldName, Class<?> type) {
		//we cannot set references to other ZooClassDefs yet, as they may not be made persistent 
		//yet
		ZooFieldDef zField = ZooFieldDef.create(this, fieldName, type);
		addFieldInternal(zField);
		return zField;
	}

	public ZooFieldDef addField(String fieldName, ZooClassDef fieldType, int arrayDepth) {
		//we cannot set references to other ZooClassDefs yet, as they may not be made persistent 
		//yet
		ZooFieldDef zField = ZooFieldDef.create(this, fieldName, fieldType, arrayDepth);
		addFieldInternal(zField);
		return zField;
	}

	private void addFieldInternal(ZooFieldDef zField) {
		localFields.add(zField);
		allFields = Arrays.copyOf(allFields, allFields.length+1);
		allFields[allFields.length-1] = zField;
		for (ZooClassDef c: getSubClasses()) {
			c.associateFields();
		}
	}
	
	public void removeField(ZooFieldDef fieldDef) {
		if (!localFields.remove(fieldDef)) {
			throw new IllegalStateException("Field not found: " + fieldDef);
		}
		for (int i = 0; i < allFields.length; i++) {
			if (allFields[i] == fieldDef) {
				if (i < allFields.length-1) {
					System.arraycopy(allFields, i+1, allFields, i, allFields.length-i-1);
				}
				allFields = Arrays.copyOf(allFields, allFields.length-1);
				break;
			}
		}
		for (ZooClassDef c: getSubClasses()) {
			c.associateFields();
		}
	}
}
