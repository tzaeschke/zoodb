package org.zoodb.jdo.internal;

import java.util.HashSet;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class ZooFieldDef {

	private long _oid;
	private String _fName;
	private String _typeName;  //TODO could be null for (_typeOid != 0)
	private long _typeOid;
	private boolean _isPersistent;  //TODO == (_typeDef==null)
	private transient ZooClassDef _typeDef;
	
	private boolean _isPrimitive = false;
	private boolean _isArray = false;
	
	private boolean _isIndexed = false;;
	private boolean _isIndexUnique;
	
	private static final HashSet<Class<?>> PRIMITIVES = new HashSet<Class<?>>();
	static {
		PRIMITIVES.add(Boolean.TYPE);
		PRIMITIVES.add(Byte.TYPE);
		PRIMITIVES.add(Character.TYPE);
		PRIMITIVES.add(Double.TYPE);
		PRIMITIVES.add(Float.TYPE);
		PRIMITIVES.add(Integer.TYPE);
		PRIMITIVES.add(Long.TYPE);
		PRIMITIVES.add(Short.TYPE);
	}
	
	public ZooFieldDef(String fieldName, Class<?> fieldType) {
		_fName = fieldName;
		_typeName = fieldType.getName();
		if (PRIMITIVES.contains(fieldType)) {
			_isPrimitive = true; //TODO does this return true for primitive arrays?
		}
		_isArray = fieldType.isArray(); //TODO store dimension instead?
		_isPersistent = PersistenceCapableImpl.class.isAssignableFrom(fieldType);
	}

	public boolean isPrimitiveType() {
		return _isPrimitive;
	}

	public boolean isPersistentType() {
		return _isPersistent;
	}

	public String getName() {
		return _fName;
	}

	public String getTypeName() {
		return _typeName;
	}
	
	public boolean isIndexed() {
		return _isIndexed;
	}
	
	public boolean isIndexUnique() {
		return _isIndexUnique;
	}

	public void setIndexed(boolean b) {
		_isIndexed = b;
	}

	public void setUnique(boolean isUnique) {
		_isIndexUnique = isUnique;
	}
}
