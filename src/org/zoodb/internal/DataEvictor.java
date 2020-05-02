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
import java.util.ArrayList;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.SerializerTools.PRIMITIVE;


/**
 * This class provides a method to nullify objects (= evicting an object) 
 * 
 * @author Tilmann Zaeschke
 */
public final class DataEvictor {

	private final Field[] refFields;
	private final ZooFieldDef[] primFields;
	
	/**
	 * Construct a data evictor that sets fields to their default values.
	 * Primitive fields are only evicted if evictPrimitives=true.
	 * @param def Class definition
	 * @param evictPrimitives Whether to evict primitives
	 */
	public DataEvictor(ZooClassDef def, boolean evictPrimitives) {
		ArrayList<Field> rfl = new ArrayList<>();
		ArrayList<ZooFieldDef> pfl = new ArrayList<>();
		for (ZooFieldDef f: def.getAllFields()) {
			if (!f.isPrimitiveType()) {
				rfl.add(f.getJavaField());
			} else if (evictPrimitives) {
				pfl.add(f);
			}
		}
		refFields = rfl.toArray(new Field[rfl.size()]);
		primFields = pfl.toArray(new ZooFieldDef[pfl.size()]);
	}
	
	
    public final void evict(ZooPC co) {
        try {
            //set reference fields
            for (int i = 0; i < refFields.length; i++) {
            	refFields[i].set(co, null);
            }
            //set primitive fields
            for (int i = 0; i < primFields.length; i++) {
            	ZooFieldDef fd = primFields[i];
                Field f = fd.getJavaField();
                evictPrimitive(co, f, fd.getPrimitiveType());
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final void evictPrimitive(Object parent, Field field, PRIMITIVE prim) 
    throws IllegalArgumentException, IllegalAccessException {
        switch (prim) {
        case BOOLEAN: field.setBoolean(parent, false); break;
        case BYTE: field.setByte(parent, (byte) 0); break;
        case CHAR: field.setChar(parent, (char) 0); break;
        case DOUBLE: field.setDouble(parent, 0); break;
        case FLOAT: field.setFloat(parent, 0); break;
        case INT: field.setInt(parent, 0); break;
        case LONG: field.setLong(parent, 0L); break;
        case SHORT: field.setShort(parent, (short) 0); break;
        default:
            throw new UnsupportedOperationException(prim.toString());
        }
    }
}
