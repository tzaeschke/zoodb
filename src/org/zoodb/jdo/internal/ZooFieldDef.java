package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class ZooFieldDef {

	static final int OFS_INIITIAL = 8 + 8; //Schema-OID + OID
	
	public enum JdoType {
		PRIMITIVE(-1),
		//Numbers are like SCOs. They cannot be indexable, because they can be 'null'!
		//Furthermore, if the type is Number, then it could be everything from boolean to double.
		NUMBER(0), 
		REFERENCE(1 + 8 + 8),
		STRING(8),
		DATE(8),
		BIG_INT(0),
		BIG_DEC(0),
		ARRAY(0),
		SCO(0);
		private final byte len;
		JdoType(int len) {
			this.len = (byte) len;
		}
		byte getLen() {
			return len;
		}
	}
	
	private final String _fName;
	private final String _typeName;
	private long _typeOid;
	private transient ZooClassDef _typeDef;
	private transient Class<?> _javaTypeDef;
	private transient Field _javaField;
	
	private JdoType _jdoType;
	
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
		PRIMITIVES.put(Boolean.class.getName(), 1);
		PRIMITIVES.put(Byte.class.getName(), 1);
		PRIMITIVES.put(Character.class.getName(), 2);
		PRIMITIVES.put(Double.class.getName(), 8);
		PRIMITIVES.put(Float.class.getName(), 4);
		PRIMITIVES.put(Integer.class.getName(), 4);
		PRIMITIVES.put(Long.class.getName(), 8);
		PRIMITIVES.put(Short.class.getName(), 2);

		PRIMITIVES.put(BigInteger.class.getName(), 0);
		PRIMITIVES.put(BigDecimal.class.getName(), 0);
		PRIMITIVES.put(Date.class.getName(), 8);
		PRIMITIVES.put(String.class.getName(), 8);
		PRIMITIVES.put(PersistenceCapableImpl.class.getName(), 1 + 8 + 8);
	}
	
	public ZooFieldDef(String name, String typeName, long typeOid, JdoType jdoType) {
		_fName = name;
		_typeName = typeName;
		_jdoType = jdoType;

//		if (_isPrimitive) {
//			_fieldLength = (byte)(int) PRIMITIVES.get(typeName);
//		} else if(_isString) {
//			_fieldLength = 8; //magic number for indexing (4 ASCII chars + 4 byte hash)
//		} else if(_isArray) {
//			_fieldLength = 4; //full array length (serialized form incl. sub-arrays)
//		} else if(_isPersistent) {
//			_fieldLength = 1 + 8 + 8;  //type byte + Schema-OID + OID
//		} else {
//			//SCO
//			_fieldLength = 1;  //The rest is stored in the appendix.
//		}
		if (_jdoType == JdoType.PRIMITIVE) {
			_fieldLength = (byte)(int)PRIMITIVES.get(typeName);
		} else {
			_fieldLength = _jdoType.getLen();
		}
	}

	public static ZooFieldDef createFromJavaType(Field jField) {
		Class<?> fieldType = jField.getType();
		JdoType jdoType;
		if (fieldType.isArray()) {
			jdoType = JdoType.ARRAY;
		} else if (fieldType.isPrimitive()) {
			jdoType = JdoType.PRIMITIVE;
		} else if (String.class.equals(fieldType)) {
			jdoType = JdoType.STRING;
		} else if (PersistenceCapableImpl.class.isAssignableFrom(fieldType)) {
			jdoType = JdoType.REFERENCE;
		} else if (Date.class.equals(fieldType)) {
			jdoType = JdoType.DATE;
		} else if (BigInteger.class.equals(fieldType)) {
			jdoType = JdoType.BIG_INT;
		} else if (BigDecimal.class.equals(fieldType)) {
			jdoType = JdoType.BIG_DEC;
		} else if (Number.class.isAssignableFrom(fieldType)) {
			jdoType = JdoType.NUMBER;
		} else {
			jdoType = JdoType.SCO;
		}
//		//TODO does this return true for primitive arrays?
//		boolean isPrimitive = PRIMITIVES.containsKey(fieldType.getName());
//		 //TODO store dimension instead?
//		boolean isArray = fieldType.isArray();
//		boolean isString = String.class.equals(fieldType);
//		//TODO does this return true for arrays?
//		boolean isPersistent = PersistenceCapableImpl.class.isAssignableFrom(fieldType);
		ZooFieldDef f = new ZooFieldDef(jField.getName(), fieldType.getName(), Long.MIN_VALUE,
				jdoType);
//				isPrimitive, isArray, isString, isPersistent);
		f.setJavaField(jField);
		return f;
	}
	
	public boolean isPrimitiveType() {
		return _jdoType == JdoType.PRIMITIVE;
	}

	public boolean isPersistentType() {
		return _jdoType == JdoType.REFERENCE;
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
		//TODO buffer in booleans
		return _jdoType == JdoType.ARRAY;
	}

	public boolean isString() {
		return _jdoType == JdoType.STRING;
	}

	public void setOffset(int ofs) {
		_offset = ofs;
	}

	public void setJavaField(Field javaField) {
		_javaField = javaField;
		_javaTypeDef = javaField.getType();
		_javaField.setAccessible(true);
	}
	
	public JdoType getJdoType() {
		return _jdoType;
	}
}
