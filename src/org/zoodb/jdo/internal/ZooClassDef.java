package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.model1p.Node1P;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

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
public class ZooClassDef {

	private final long _oid;
	private final String _className;
	private transient Class<?> _cls;
	
	private final long _oidSuper;
	private transient ZooClassDef _super;
	private transient List<ZooClassDef> _subs = new ArrayList<ZooClassDef>();
	private transient ISchema _apiHandle = null;
	
	private final List<ZooFieldDef> _localFields = new ArrayList<ZooFieldDef>(10);
	//private final List<ZooFieldDef> _allFields = new ArrayList<ZooFieldDef>(10);
	private ZooFieldDef[] _allFields;
	private transient Map<String, ZooFieldDef> fieldBuffer = null;
	
	public ZooClassDef(String clsName, long oid, long superOid) {
		_oid = oid;
		_className = clsName;
		_oidSuper = superOid;
	}
	
	public static ZooClassDef createFromJavaType(Class<?> cls, long oid, ZooClassDef defSuper) {
        //create instance
        ZooClassDef def;
        long superOid = 0;
        if (cls != PersistenceCapableImpl.class) {
            superOid = defSuper.getOid();
            if (superOid == 0) {
                throw new IllegalStateException("No super class found: " + cls.getName());
            }
        }
        def = new ZooClassDef(cls.getName(), oid, superOid);

        //local fields:
		List<ZooFieldDef> fieldList = new ArrayList<ZooFieldDef>();
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
		def._cls = cls;
		def.associateSuperDef(defSuper);
		def.associateFields();
		
		return def;
	}
	
	void addFields(List<ZooFieldDef> fieldList) {
        _localFields.addAll(fieldList);
    }

    public void associateFCOs(Node1P node, Collection<CachedObject.CachedSchema> cachedSchemata) {
		//Fields:
		for (ZooFieldDef zField: _localFields) {
			String typeName = zField.getTypeName();
			
			if (zField.isPrimitiveType()) {
				//no further work for primitives
				continue;
			}
			
			ZooClassDef typeDef = null;
			
			for (CachedObject.CachedSchema cs: cachedSchemata) {
				if (cs.getSchema().getClassName().equals(typeName)) {
					typeDef = cs.getSchema();
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
		return _className;
	}

	public long getOid() {
		return _oid;
	}
	
	public Class<?> getJavaClass() {
		return _cls;
	}

	public void associateJavaTypes() {
		if (_cls != null) {
			throw new IllegalStateException();
		}
		
		String fName = null;
		try {
			_cls = Class.forName(_className);
			for (ZooFieldDef f: _localFields) {
				fName = f.getName();
				Field jf = _cls.getDeclaredField(fName);
				f.setJavaField(jf);
			}
		} catch (ClassNotFoundException e) {
		    //TODO this in only for checkDB ...
		    System.err.println("Class not found: " + _className);
		    return;
			//throw new JDOFatalDataStoreException("Class not found: " + _className, e);
		} catch (SecurityException e) {
			throw new JDOFatalDataStoreException("No access to class fields: " + _className + "." +
					fName, e);
		} catch (NoSuchFieldException e) {
			throw new JDOUserException("Schema error, field not found in Java class: " + 
					_className + "." + fName, e);
		}

		// We check field mismatches and missing Java fields above. 
		// Now check field count, this should cover missing schema fields (too many Java fields).
		// we need to filter out transient and static fields
		int n = 0;
		for (Field f: _cls.getDeclaredFields()) {
			int mod = f.getModifiers();
			if (Modifier.isTransient(mod) || Modifier.isStatic(mod)) {
				continue;
			}
			n++;
		}
		if (_localFields.size() != n) {
			throw new JDOUserException("Schema error, field count mismatch between Java class (" +
					n + ") and database class (" + _localFields.size() + ").");
		}
	}

	public List<ZooFieldDef> getLocalFields() {
		return _localFields;
	}

	public ZooFieldDef[] getAllFields() {
		return _allFields;
	}

	public ISchema getApiHandle() {
		return _apiHandle;
	}
	
	public void setApiHandle(ISchema handle) {
		_apiHandle = handle;
	}


	
	public long getSuperOID() {
		return _oidSuper;
	}

	/**
	 * Only to be used during database startup to load the schema-tree.
	 * @param superDef
	 */
	public void associateSuperDef(ZooClassDef superDef) {
		if (_super != null) {
			throw new IllegalStateException();
		}

		//For PersistenceCapableImpl this may be null:
		if (superDef != null) {
			//class invariant
			if (superDef.getOid() != _oidSuper) {
				throw new IllegalStateException("s-oid= " + _oidSuper + " / " + superDef.getOid() + 
						"  class=" + _className);
			}
			superDef.addSubClass(this);
		}

		_super = superDef;
	}

	public void associateFields() {
		LinkedList<ZooFieldDef> allFields = new LinkedList<ZooFieldDef>();
		
		//For PersistenceCapableImpl _super may be null:
		ZooClassDef sup = _super;
		while (sup != null) {
			allFields.addAll(0, sup.getLocalFields());
			sup = sup._super;
		}

		int ofs = ZooFieldDef.OFS_INIITIAL; //8 + 8; //Schema-OID + OID
		if (!allFields.isEmpty()) {
			ofs = allFields.get(allFields.size()-1).getNextOffset();
		}

		//local fields:
		for (ZooFieldDef f: _localFields) {
			f.setOffset(ofs);
			ofs = f.getNextOffset();
			allFields.add(f);
		}
		
		_allFields = allFields.toArray(new ZooFieldDef[allFields.size()]);
	}

	public ZooFieldDef getField(String attrName) {
		for (ZooFieldDef f: _allFields) {
			if (f.getName().equals(attrName)) {
				return f;
			}
		}
		throw new JDOUserException("Field name not found: " + attrName);
	}

	private void addSubClass(ZooClassDef sub) {
		_subs.add(sub);
	}
	
	public List<ZooClassDef> getSubClasses() {
		return _subs;
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
		if (_super == cls) {
			return true;
		}
		if (_super == null) {
			return false;
		}
		return _super.hasSuperClass(cls);
	}
}
