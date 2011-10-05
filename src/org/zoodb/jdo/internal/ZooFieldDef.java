package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;

import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.SerializerTools.PRIMITIVE;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class ZooFieldDef {

	public static final int OFS_INIITIAL = 8; //OID
	
	public enum JdoType {
		PRIMITIVE(-1, true),
		//Numbers are like SCOs. They cannot be indexable, because they can be 'null'!
		//Furthermore, if the type is Number, then it could be everything from boolean to double.
		NUMBER(0, false), 
		REFERENCE(1+1 + 8 + 8, true),
		STRING(1+8, true),
		DATE(1+8, true),
		BIG_INT(0, false),
		BIG_DEC(0, false),
		ARRAY(0, false),
		SCO(0, false);
		private final byte len;
		private final boolean fixedSize;
		JdoType(int len, boolean fixedSize) {
			this.len = (byte) len;
			this.fixedSize = fixedSize;
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

	private final transient ZooClassDef _declaringType;

	private JdoType _jdoType;
	
	private boolean _isIndexed = false;;
	private boolean _isIndexUnique;
	
	private int _offset = Integer.MIN_VALUE;
	private final byte _fieldLength;
	private final boolean _isFixedSize;
	
	private final PRIMITIVE _primitive;
	
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
	
	public ZooFieldDef(ZooClassDef declaringType,
	        String name, String typeName, long typeOid, JdoType jdoType) {
	    _declaringType = declaringType;
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
			PRIMITIVE prim = null;
			for (PRIMITIVE p: PRIMITIVE.values()) {
				if (p.name().equals(typeName.toUpperCase())) {
					prim = p;
					break;
				}
			}
			//_primitive = SerializerTools.PRIMITIVE_TYPES.get(Class.forName(typeName));
			if ((_primitive = prim) == null) {
				throw new RuntimeException("Primitive type not found: " + typeName);
			}
		} else {
			_fieldLength = _jdoType.getLen();
			_primitive = null;
		}
		_isFixedSize = _jdoType.fixedSize;
	}

	public static ZooFieldDef createFromJavaType(ZooClassDef declaringType, Field jField) {
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
		ZooFieldDef f = new ZooFieldDef(declaringType, jField.getName(), fieldType.getName(), 
		        Long.MIN_VALUE, jdoType);
//				isPrimitive, isArray, isString, isPersistent);
		f.setJavaField(jField);
		return f;
	}
	
	public PRIMITIVE getPrimitiveType() {
		return _primitive;
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

	public int getLength() {
		return _fieldLength;
	}
	
	public boolean isFixedSize() {
		return _isFixedSize;
	}

	public boolean isDate() {
		return _jdoType == JdoType.DATE;
	}

	public long getMinValue() {
		if (isPrimitiveType()) {
			switch(getPrimitiveType()) {
			case BOOLEAN: return 0;
			case BYTE: return Byte.MIN_VALUE;
			case CHAR: return Character.MIN_VALUE;
			case DOUBLE: return BitTools.toSortableLong(Double.MIN_VALUE);
			case FLOAT: return BitTools.toSortableLong(Float.MIN_VALUE);
			case INT: return Integer.MIN_VALUE;
			case LONG: return Long.MIN_VALUE;
			case SHORT: return Short.MIN_VALUE;
			}
		}
		if (isString()) {
			return Long.MIN_VALUE;  //TODO is this correct? Can it be negative?
		}
		if (isDate()) {
			return 0;  //TODO is this correct?
		}
		throw new JDOUserException("Type not supported in query: " + _typeName);
	}

	public long getMaxValue() {
		if (isPrimitiveType()) {
			switch(getPrimitiveType()) {
			case BOOLEAN: return 0;
			case BYTE: return Byte.MAX_VALUE;
			case CHAR: return Character.MAX_VALUE;
			case DOUBLE: return BitTools.toSortableLong(Double.MAX_VALUE);
			case FLOAT: return BitTools.toSortableLong(Float.MAX_VALUE);
			case INT: return Integer.MAX_VALUE;
			case LONG: return Long.MAX_VALUE;
			case SHORT: return Short.MAX_VALUE;
			}
		}
		if (isString()) {
			return Long.MAX_VALUE;
		}
		if (isDate()) {
			return Long.MAX_VALUE;  //TODO is this correct?
		}
		throw new JDOUserException("Type not supported in query: " + _typeName);
	}
	
	public ZooClassDef getDeclaringType() {
	    return _declaringType;
	}
}
