/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal;

/**
 * Persistent schema evolution operation. These are used to evolve objects on the fly when they 
 * are loaded.
 * 
 * 
 * @author Tilmann Zaschke
 */
public class PersistentSchemaOperation {

	public static enum OP {
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
