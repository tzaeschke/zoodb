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

/**
 * Persistent schema evolution operation. These are used to evolve objects on the fly when they 
 * are loaded.
 * 
 * 
 * @author Tilmann Zaschke
 */
public class PersistentSchemaOperation {

	public enum OP {
		ADD,
		REMOVE;
	}

	private final OP op;
	private final int fieldId;
	
	private final ZooFieldDef field;
	//TODO make this a byte[]?
	private final Object initialValue;
	private final byte[] evolutionFunction = null;
	
	private PersistentSchemaOperation() {
		//private constructor only for ZooDB persistence. DO NOT USE.
		op = null;
		fieldId = -1;
		field = null;
		initialValue = null;
	}
	
	private PersistentSchemaOperation(OP op, int fieldId) {
		this.op = op;
		this.fieldId = fieldId;
		this.field = null;
		this.initialValue = null;
	}
	
	private PersistentSchemaOperation(OP op, int fieldId, ZooFieldDef field, Object initialValue) {
		this.op = op;
		this.fieldId = fieldId;
		this.field = field;
		if (initialValue == null) {
			initialValue = getDefaultValue(field);
		}
		this.initialValue = initialValue;
	}
	
	public static Object getDefaultValue(ZooFieldDef field) {
		switch (field.getJdoType()) {
		case PRIMITIVE:
			switch (field.getPrimitiveType()) {
			case BOOLEAN: return Boolean.valueOf(false);
			case BYTE: return Byte.valueOf((byte) 0);
			case CHAR: return Character.valueOf((char) 0);
			case DOUBLE: return Double.valueOf(0);
			case FLOAT: return Float.valueOf(0);
			case INT: return Integer.valueOf(0);
			case LONG: return Long.valueOf(0L);
			case SHORT: return Short.valueOf((short) 0);
			default: throw new IllegalArgumentException();
			}
		case DATE:
		case REFERENCE:
		case NUMBER:
		case ARRAY:
		case BIG_DEC:
		case BIG_INT:
		case SCO:
		case STRING: 
			return null;
		default: throw new IllegalArgumentException();
		}
	}
	
	public static PersistentSchemaOperation newAddOperation(int fieldId, ZooFieldDef field, 
			Object initialValue) {
		return new PersistentSchemaOperation(OP.ADD, fieldId, field, initialValue);
	}
	
	public static PersistentSchemaOperation newRemoveOperation(int fieldId) {
		return new PersistentSchemaOperation(OP.REMOVE, fieldId);
	}
	
	public boolean isAddOp() {
		return op == OP.ADD;
	}
	
	public int getFieldId() {
		return fieldId;
	}
	
	public Object getInitialValue() {
		return initialValue;
	}

    public ZooFieldDef getField() {
        return field;
    }
}
