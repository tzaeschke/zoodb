package org.zoodb.jdo.internal;

import java.util.HashMap;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class ZooFieldDef {

	private final String _fName;
	private final String _typeName;  //TODO could be null for (_typeOid != 0)
	private long _typeOid;
	private boolean _isPersistent;  //TODO == (_typeDef==null)
	private transient ZooClassDef _typeDef;
	
	private final boolean _isPrimitive;
	private final boolean _isArray;
	//special treatment for String because they are indexable?
	private final boolean _isString;
	
	private boolean _isIndexed = false;;
	private boolean _isIndexUnique;
	
	private final int _offset;
	private final byte _fieldLength;
	
	private static final HashMap<Class<?>, Integer> PRIMITIVES = new HashMap<Class<?>, Integer>();
	static {
		PRIMITIVES.put(Boolean.TYPE, 1);
		PRIMITIVES.put(Byte.TYPE, 1);
		PRIMITIVES.put(Character.TYPE, 2);
		PRIMITIVES.put(Double.TYPE, 8);
		PRIMITIVES.put(Float.TYPE, 4);
		PRIMITIVES.put(Integer.TYPE, 4);
		PRIMITIVES.put(Long.TYPE, 8);
		PRIMITIVES.put(Short.TYPE, 2);
	}
	
	public ZooFieldDef(String fieldName, Class<?> fieldType, int offset) {
		_fName = fieldName;
		_typeName = fieldType.getName();
		//TODO does this return true for primitive arrays?
		_isPrimitive = PRIMITIVES.containsKey(fieldType);
		 //TODO store dimension instead?
		_isArray = fieldType.isArray();
		_isString = String.class.equals(fieldType);
		//TODO does this return true for arrays?
		_isPersistent = PersistenceCapableImpl.class.isAssignableFrom(fieldType);
		_offset = offset;

		if (_isPrimitive) {
			_fieldLength = (byte)(int) PRIMITIVES.get(fieldType);
		} else if(_isArray || _isString) {
			_fieldLength = 4; //full array length (serialized form incl. sub-arrays)
		} else if(_isPersistent) {
			_fieldLength = 1 + 8 + 8;  //type byte + Schema-OID + OID
		} else {
			//SCO
			_fieldLength = 1;  //The rest is stored in the appendix.
		}
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
	
	protected int getNextOffset() {
		return _offset + _fieldLength; 
	}

	public int getOffset() {
		return _offset;
	}
}
