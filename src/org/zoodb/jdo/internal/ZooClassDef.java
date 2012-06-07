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
import org.zoodb.jdo.internal.model1p.Node1P;
import org.zoodb.jdo.internal.util.ClassCreator;
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
	//private final ArrayList<ZooFieldDef> _allFields = new ArrayList<ZooFieldDef>(10);
	private transient ZooFieldDef[] allFields;
	private transient Map<String, ZooFieldDef> fieldBuffer = null;
	private transient PCContext providedContext = null;
	
	private ZooClassDef(String clsName, long oid, long superOid) {
		jdoZooSetOid(oid);
		this.className = clsName;
		this.oidSuper = superOid;
	}
	
	/**
	 * Methods used for bootstrapping the schema of newly created databases.
	 * @return Root schema
	 */
	public static ZooClassDef bootstrapZooPCImpl(long oid) {
		return new ZooClassDef(ZooPCImpl.class.getName(), oid, 0); 
	}
	
	/**
	 * Methods used for bootstrapping the schema of newly created databases.
	 * @return Meta schema instance
	 */
	public static ZooClassDef bootstrapZooClassDef(long oid, long oidSuper) {
		ZooClassDef meta = new ZooClassDef(ZooClassDef.class.getName(), oid, oidSuper);
		ArrayList<ZooFieldDef> fields = new ArrayList<ZooFieldDef>();
		fields.add(new ZooFieldDef(meta, "className", String.class.getName(), JdoType.STRING));
		fields.add(new ZooFieldDef(meta, "oidSuper", long.class.getName(), JdoType.PRIMITIVE));
		fields.add(new ZooFieldDef(meta, "localFields", ArrayList.class.getName(), JdoType.SCO));
		//new ZooFieldDef(this, allFields, ZooFieldDef[].class.getName(), typeOid, JdoType.ARRAY);
		meta.addFields(fields);
		return meta;
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
		def.addFields(fieldList);
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
	
	void addFields(List<ZooFieldDef> fieldList) {
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
			throw new IllegalStateException();
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
		    System.err.println("Class not found: " + className);
		    cls = ClassCreator.createClass(className);
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
		return apiHandle;
	}
	
	public void setApiHandle(ISchema handle) {
		this.apiHandle = handle;
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
}
