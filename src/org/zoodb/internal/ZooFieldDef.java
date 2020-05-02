/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.SerializerTools.PRIMITIVE;
import org.zoodb.internal.server.index.BitTools;
import org.zoodb.internal.util.DBLogger;

public class ZooFieldDef {

	public static final int BYTES_OF_OID = 8; //length of OID
	public static final int BYTES_OF_SCHEMA_OID = 8; //lengths of Schema-OID
	public static final int OFS_INIITIAL = BYTES_OF_OID + BYTES_OF_SCHEMA_OID; //OID + Schema-OID
	
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
	
	private String fName;
	private final long schemaId;
	private final String typeName;
	private long typeOid;
	private final int arrayDim;
	private transient ZooClassDef typeDef;
	private transient Class<?> javaTypeDef;
	private transient Field javaField;

	private final ZooClassDef declaringType;

	private JdoType jdoType;
	
	private boolean isIndexed = false;
    private boolean isIndexUnique;
	
	private int offset = Integer.MIN_VALUE;
    private int fieldPos = -1;
	private final byte fieldLength;
	private final boolean isFixedSize;
	
	private final PRIMITIVE primitive;
	private transient ZooFieldProxy proxy = null;
	
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
		PRIMITIVES.put(ZooPC.class.getName(), 1 + 8 + 8);
	}
	
	@SuppressWarnings("unused")
	private ZooFieldDef () {
		//private constructor for de-serializer only!
		typeName = null;
		primitive = null;
		isFixedSize = false;
		fieldLength = 0;
		fName = null;
		declaringType = null;
		schemaId = -1;
		arrayDim = 0;
	}

	/**
	 * Copy constructor.
	 */
	ZooFieldDef (ZooFieldDef f, ZooClassDef declaringClass) {
		//private constructor for de-serializer only!
		fName = f.fName; 
		typeName = f.typeName;
		primitive = f.primitive;
		isFixedSize = f.isFixedSize;
		fieldLength = f.fieldLength;
		declaringType = declaringClass;
		typeOid = f.typeOid;
		typeDef = f.typeDef;
		javaTypeDef = f.javaTypeDef;
		javaField = f.javaField;
		jdoType = f.jdoType;
		isIndexed = f.isIndexed;
		isIndexUnique = f.isIndexUnique;
		offset = f.offset;
		fieldPos = f.fieldPos;
		proxy = f.proxy;
		schemaId = f.schemaId;
		arrayDim = f.arrayDim;
	}

	ZooFieldDef(ZooClassDef declaringType, String name, String typeName, int arrayDim,
			JdoType jdoType, long oid) {
		this.declaringType = declaringType;
	    this.fName = name;
		this.typeName = typeName;
		this.jdoType = jdoType;
		this.arrayDim = arrayDim;
		//This is not a new version but a new field, so the schemId equals the OID. 
		//TODO remove this in case Field becomes a PC. Then use the actual OID
		this.schemaId = oid;

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
		if (this.jdoType == JdoType.PRIMITIVE) {
			fieldLength = (byte)(int)PRIMITIVES.get(typeName);
			PRIMITIVE prim = getPrimitiveType(typeName);
			//_primitive = SerializerTools.PRIMITIVE_TYPES.get(Class.forName(typeName));
			if ((primitive = prim) == null) {
				throw new RuntimeException("Primitive type not found: " + typeName);
			}
		} else {
			this.fieldLength = this.jdoType.getLen();
			this.primitive = null;
		}
		this.isFixedSize = this.jdoType.fixedSize;
	}

	private PRIMITIVE getPrimitiveType(String typeName) {
		for (PRIMITIVE p: PRIMITIVE.values()) {
			if (p.name().equals(typeName.toUpperCase())) {
				return p;
			}
		}
		return null;
	}
	
	public static ZooFieldDef createFromJavaType(ZooClassDef declaringType, Field jField, 
			long fieldOid) {
		Class<?> fieldType = jField.getType();
////		//TODO does this return true for primitive arrays?
////		boolean isPrimitive = PRIMITIVES.containsKey(fieldType.getName());
////		 //TODO store dimension instead?
////		boolean isArray = fieldType.isArray();
////		boolean isString = String.class.equals(fieldType);
////		//TODO does this return true for arrays?
////		boolean isPersistent = PersistenceCapableImpl.class.isAssignableFrom(fieldType);
//		ZooFieldDef f = new ZooFieldDef(declaringType, jField.getName(), fieldType.getName(), 
//		        jdoType);
////				isPrimitive, isArray, isString, isPersistent);
		ZooFieldDef f = create(declaringType, jField.getName(), fieldType, fieldOid);
		f.setJavaField(jField);
		return f;
	}

	public static ZooFieldDef create(ZooClassDef declaringType, String fieldName,
			Class<?> fieldType, long fieldOid) {
		if (ObjectGraphTraverser.ILLEGAL_TYPES.contains(fieldType)) {
			throw DBLogger.newUser("Class fields of this type cannot be stored. Could they be "
					+ "made 'static' or 'transient'? Type: " + fieldType + " in " 
					+ declaringType.getClassName() + "." + fieldName);
		}
		String typeName = fieldType.getName();
		JdoType jdoType = getJdoType(fieldType);
		int arrayDim = 0;
		for (int i = 0; i < typeName.length(); i++) {
			if (typeName.charAt(i) == '[') {
				arrayDim++;
			}
		}
		return new ZooFieldDef(declaringType, fieldName, typeName, arrayDim, jdoType, fieldOid);
	}

	static JdoType getJdoType(Class<?> fieldType) {
		JdoType jdoType;
		if (fieldType.isArray()) {
			jdoType = JdoType.ARRAY;
		} else if (fieldType.isPrimitive()) {
			jdoType = JdoType.PRIMITIVE;
		} else if (String.class.equals(fieldType)) {
			jdoType = JdoType.STRING;
		} else if (ZooPC.class.isAssignableFrom(fieldType)) {
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
		return jdoType;
	}
	
	/**
	 * Creates references and reference arrays  to persistent classes.
	 * @param declaringType the type that contains the field
	 * @param fieldName the name of the field
	 * @param fieldType The ZooClassDef of the target class of a reference.
	 * @param arrayDim the dimensionality of the array (if the field is an array)
	 * @return ZooFieldDef
	 */
	public static ZooFieldDef create(ZooClassDef declaringType, String fieldName,
			ZooClassDef fieldType, int arrayDim) {
		String typeName = fieldType.getClassName();
		JdoType jdoType;
		if (arrayDim > 0) {
			jdoType = JdoType.ARRAY;
		} else {
			jdoType = JdoType.REFERENCE;
		}
        long fieldOid = declaringType.jdoZooGetNode().getOidBuffer().allocateOid();
		return new ZooFieldDef(declaringType, fieldName, typeName, arrayDim, jdoType, fieldOid);
	}
	
	public PRIMITIVE getPrimitiveType() {
		return primitive;
	}
	
	public boolean isPrimitiveType() {
		return jdoType == JdoType.PRIMITIVE;
	}

	public boolean isPersistentType() {
		return jdoType == JdoType.REFERENCE;
	}
	
	public void setType(ZooClassDef clsDef) {
		this.typeDef = clsDef;
		this.typeOid = clsDef.getOid();
	}
	
	public ZooClassDef getType() {
		return typeDef;
	}

	public String getName() {
		return fName;
	}

	public String getTypeName() {
		return typeName;
	}
	
	public boolean isIndexed() {
		return isIndexed;
	}
	
	public boolean isIndexUnique() {
		return isIndexUnique;
	}
	
	public void setIndexed(boolean b) {
		isIndexed = b;
	}

	public void setUnique(boolean isUnique) {
		isIndexUnique = isUnique;
	}
	
	protected int getNextOffset() {
		return offset + fieldLength; 
	}

    public int getOffset() {
        return offset;
    }

    /**
     * 
     * @return The position of the field. This counts from [0.. nFields-1].
     */
    public int getFieldPos() {
        return fieldPos;
    }

	public Class<?> getJavaType() {
		return javaTypeDef;
	}

	public Field getJavaField() {
		return javaField;
	}

	public long getTypeOID() {
		return typeOid;
	}

	public boolean isArray() {
		//TODO buffer in booleans
		return jdoType == JdoType.ARRAY;
	}

	public boolean isString() {
		return jdoType == JdoType.STRING;
	}

	public void setOffset(int ofs, int fieldPos) {
		this.offset = ofs;
		this.fieldPos = fieldPos;
	}

	public void setJavaField(Field javaField) {
		checkField(javaField);
		this.javaField = javaField;
		this.javaTypeDef = javaField.getType();
		this.javaField.setAccessible(true);
	}
	
	private void checkField(Field javaField) {
		if (!javaField.getName().equals(fName)) {
			throw DBLogger.newUser(
					"Field name mismatch: " + fName + " <-> " + javaField.getName());
		}
		JdoType jdoType = getJdoType(javaField.getType());
		if (jdoType != this.jdoType) {
			throw DBLogger.newUser("Field type mismatch: " + this.jdoType + " <-> " + jdoType);
		}
		if (jdoType == JdoType.PRIMITIVE) {
			PRIMITIVE prim = getPrimitiveType(javaField.getType().getName());
			if (prim != this.primitive) {
				throw DBLogger.newUser("Field type mismatch: " + this.primitive + " <-> " + prim);
			}
		}
	}
	
	public void unsetJavaField() {
		this.javaField = null;
		this.javaTypeDef = null;
	}
	
	public JdoType getJdoType() {
		return jdoType;
	}

	public int getLength() {
		return fieldLength;
	}
	
	public boolean isFixedSize() {
		return isFixedSize;
	}

	public boolean isDate() {
		return jdoType == JdoType.DATE;
	}

	public long getMinValue() {
		if (isPrimitiveType()) {
			switch(getPrimitiveType()) {
			case BOOLEAN: return 0;
			case BYTE: return Byte.MIN_VALUE;
			case CHAR: return Character.MIN_VALUE;
			case DOUBLE: return BitTools.toSortableLong(Double.NEGATIVE_INFINITY);
			case FLOAT: return BitTools.toSortableLong(Float.NEGATIVE_INFINITY);
			case INT: return Integer.MIN_VALUE;
			case LONG: return Long.MIN_VALUE;
			case SHORT: return Short.MIN_VALUE;
			}
		}
		if (isString() || isPersistentType()) {
			return Long.MIN_VALUE;  //TODO is this correct? Can it be negative?
		}
		if (isDate()) {
			return 0;  //TODO is this correct?
		}
		throw DBLogger.newUser("Type not supported in query: " + typeName);
	}

	public long getMaxValue() {
		if (isPrimitiveType()) {
			switch(getPrimitiveType()) {
			case BOOLEAN: return 0;
			case BYTE: return Byte.MAX_VALUE;
			case CHAR: return Character.MAX_VALUE;
			case DOUBLE: return BitTools.toSortableLong(Double.POSITIVE_INFINITY);
			case FLOAT: return BitTools.toSortableLong(Float.POSITIVE_INFINITY);
			case INT: return Integer.MAX_VALUE;
			case LONG: return Long.MAX_VALUE;
			case SHORT: return Short.MAX_VALUE;
			}
		}
		if (isString() || isPersistentType()) {
			return Long.MAX_VALUE;
		}
		if (isDate()) {
			return Long.MAX_VALUE;  //TODO is this correct?
		}
		throw DBLogger.newUser("Type not supported in query: " + typeName);
	}
	
	public ZooClassDef getDeclaringType() {
	    return declaringType;
	}

	public ZooFieldProxy getProxy() {
		if (proxy == null) {
			proxy = new ZooFieldProxy(this, 
					declaringType.jdoZooGetContext().getSession().getSchemaManager());
		}
		return proxy;
	}
	
	@Override
	public String toString() {
		return "Field: " + declaringType.getClassName() + "." + fName;
	}

	public void updateName(String fieldName) {
		this.fName = fieldName;
		declaringType.rebuildFieldsRecursive();
	}

	/**
	 * 
	 * @return The unique ID of this field, which stays the same for different versions of the 
	 * field.
	 */
    public long getFieldSchemaId() {
        return schemaId;
    }

	public int getArrayDim() {
		return arrayDim;
	}
}
