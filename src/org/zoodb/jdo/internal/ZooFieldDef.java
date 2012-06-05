/*
 * Copyright 2009-2011 Tilmann Z�schke. All rights reserved.
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;

import javax.jdo.JDOUserException;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.internal.SerializerTools.PRIMITIVE;
import org.zoodb.jdo.internal.server.index.BitTools;

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
	
	private final String fName;
	private final String typeName;
	private long typeOid;
	private transient ZooClassDef typeDef;
	private transient Class<?> javaTypeDef;
	private transient Field javaField;

	private final transient ZooClassDef declaringType;

	private JdoType jdoType;
	
	private boolean isIndexed = false;;
	private boolean isIndexUnique;
	
	private int offset = Integer.MIN_VALUE;
	private final byte fieldLength;
	private final boolean isFixedSize;
	
	private final PRIMITIVE primitive;
	
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
		PRIMITIVES.put(ZooPCImpl.class.getName(), 1 + 8 + 8);
	}
	
	public ZooFieldDef(ZooClassDef declaringType,
	        String name, String typeName, long typeOid, JdoType jdoType) {
		this.declaringType = declaringType;
	    this.fName = name;
		this.typeName = typeName;
		this.jdoType = jdoType;

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
			PRIMITIVE prim = null;
			for (PRIMITIVE p: PRIMITIVE.values()) {
				if (p.name().equals(typeName.toUpperCase())) {
					prim = p;
					break;
				}
			}
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

	public static ZooFieldDef createFromJavaType(ZooClassDef declaringType, Field jField) {
		Class<?> fieldType = jField.getType();
		JdoType jdoType;
		if (fieldType.isArray()) {
			jdoType = JdoType.ARRAY;
		} else if (fieldType.isPrimitive()) {
			jdoType = JdoType.PRIMITIVE;
		} else if (String.class.equals(fieldType)) {
			jdoType = JdoType.STRING;
		} else if (ZooPCImpl.class.isAssignableFrom(fieldType)) {
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
		return primitive;
	}
	
	public boolean isPrimitiveType() {
		return jdoType == JdoType.PRIMITIVE;
	}

	public boolean isPersistentType() {
		return jdoType == JdoType.REFERENCE;
	}
	
	void setType(ZooClassDef clsDef) {
		this.typeDef = clsDef;
		this.typeOid = typeDef.getOid();
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

	public void setOffset(int ofs) {
		offset = ofs;
	}

	public void setJavaField(Field javaField) {
		this.javaField = javaField;
		this.javaTypeDef = javaField.getType();
		this.javaField.setAccessible(true);
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
		throw new JDOUserException("Type not supported in query: " + typeName);
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
		throw new JDOUserException("Type not supported in query: " + typeName);
	}
	
	public ZooClassDef getDeclaringType() {
	    return declaringType;
	}
}
