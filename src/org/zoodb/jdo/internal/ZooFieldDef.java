package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class ZooFieldDef {

	private final String _fName;
	private final String _typeName;
	private long _typeOid;
	private boolean _isPersistent;
	private transient ZooClassDef _typeDef;
	private transient Class<?> _javaTypeDef;
	private transient Field _javaField;
	
	private final boolean _isPrimitive;
	private final boolean _isArray;
	//special treatment for String because they are indexable?
	private final boolean _isString;
	
	private boolean _isIndexed = false;;
	private boolean _isIndexUnique;
	
	private int _offset = Integer.MIN_VALUE;
	private final byte _fieldLength;
	
	private static final HashMap<String, Integer> PRIMITIVES = new HashMap<String, Integer>();
	static {
		PRIMITIVES.put(Boolean.TYPE.getName(), 1);
		PRIMITIVES.put(Byte.TYPE.getName(), 1);
		PRIMITIVES.put(Character.TYPE.getName(), 2);
		PRIMITIVES.put(Double.TYPE.getName(), 8);
		PRIMITIVES.put(Float.TYPE.getName(), 4);
		PRIMITIVES.put(Integer.TYPE.getName(), 4);
		PRIMITIVES.put(Long.TYPE.getName(), 8);
		PRIMITIVES.put(Short.TYPE.getName(), 2);
	}
	
	public ZooFieldDef(String name, String typeName, long typeOid, boolean isPrimitive, 
			boolean isArray, boolean isString, boolean isPersistenCapable) {
		_fName = name;
		_typeName = typeName;
		_isPrimitive = isPrimitive;
		_isArray = isArray;
		_isString = isString;
		_isPersistent = isPersistenCapable;

		if (_isPrimitive) {
			_fieldLength = (byte)(int) PRIMITIVES.get(typeName);
		} else if(_isArray || _isString) {
			_fieldLength = 4; //full array length (serialized form incl. sub-arrays)
		} else if(_isPersistent) {
			_fieldLength = 1 + 8 + 8;  //type byte + Schema-OID + OID
		} else {
			//SCO
			_fieldLength = 1;  //The rest is stored in the appendix.
		}
	}

	public static ZooFieldDef createFromJavaType(Field jField) {
		Class<?> fieldType = jField.getType();
		//TODO does this return true for primitive arrays?
		boolean isPrimitive = PRIMITIVES.containsKey(fieldType.getName());
		 //TODO store dimension instead?
		boolean isArray = fieldType.isArray();
		boolean isString = String.class.equals(fieldType);
		//TODO does this return true for arrays?
		boolean isPersistent = PersistenceCapableImpl.class.isAssignableFrom(fieldType);
		ZooFieldDef f = new ZooFieldDef(jField.getName(), fieldType.getName(), Long.MIN_VALUE, 
				isPrimitive, isArray, isString, isPersistent);
		f.setJavaField(jField);
		return f;
	}
	
	public boolean isPrimitiveType() {
		return _isPrimitive;
	}

	public boolean isPersistentType() {
		return _isPersistent;
	}
	
	void setType(ZooClassDef clsDef) {
		_typeDef = clsDef;
		_typeOid = _typeDef.getOid();
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

	public Class<?> getJavaType() {
		return _javaTypeDef;
	}

	public Field getJavaField() {
		return _javaField;
	}

	public long getTypeOID() {
		return _typeOid;
	}

	public boolean isArray() {
		return _isArray;
	}

	public boolean isString() {
		return _isString;
	}

	public void setOffset(int ofs) {
		_offset = ofs;
	}

	public void setJavaField(Field javaField) {
		_javaField = javaField;
		_javaTypeDef = javaField.getType();
		_javaField.setAccessible(true);
	}
}
