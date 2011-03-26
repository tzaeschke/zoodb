package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.model1p.Node1P;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * ZooClassDef represents a class schema definition used by the database. 
 * The highest stored schema is that of PersistenceCapableImpl.
 * 
 * @author Tilmann Zäschke
 */
public class ZooClassDef {

	private final long _oid;
	private final String _className;
	private transient Class<?> _cls;
	
	private final long _oidSuper;
	private transient ZooClassDef _super;
	private transient ISchema _apiHandle = null;
	
	private final List<ZooFieldDef> _fields = new LinkedList<ZooFieldDef>();
	
	public ZooClassDef(Class<?> cls, long oid, ZooClassDef defSuper, long superOid) {
		_oid = oid;
		_className = cls.getName();
		_cls = cls;
		
		if (superOid == 0 && cls != PersistenceCapableImpl.class) {
			throw new IllegalStateException("No super class found: " + cls.getName());
		}

		_oidSuper = superOid;
		//During group load and for PersistenceCapableImpl, this may be null:
		_super = defSuper;

		//Fields:
		//TODO does this return only local fields. Is that correct? -> Information units.
		Field[] fields = _cls.getDeclaredFields(); 
		for (int i = 0; i < fields.length; i++) {
			Field jField = fields[i];
			if (Modifier.isStatic(jField.getModifiers()) || 
					Modifier.isTransient(jField.getModifiers())) {
				continue;
			}
			Class<?> jType = jField.getType();
			String fName = jField.getName();
			//we cannot set references to other ZooClassDefs yet, as they may not be made persistent 
			//yet
			ZooFieldDef zField = new ZooFieldDef(fName, jType);
			_fields.add(zField);
		}		
	}
	
	
	public void constructFields(Node1P node, List<CachedObject.CachedSchema> cachedSchemata) {
		//Fields:
		for (ZooFieldDef zField: _fields) {
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
			}
			
			//TODO what is this good for?
		}
	}
	
	public String getClassName() {
		return _className;
	}

	public long getOid() {
		return _oid;
	}
	
	public Class<?> getSchemaClass() {
		return _cls;
	}

	public ZooFieldDef[] getFields() {
		return _fields.toArray(new ZooFieldDef[_fields.size()]);
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
	public void setSuperDef(ZooClassDef superDef) {
		//class invariant
		if (superDef.getOid() != _oidSuper) {
			throw new IllegalStateException("s-oid= " + _oidSuper + " / " + superDef.getOid() + 
					"  class=" + _className);
		}
		_super = superDef;
	}
}
