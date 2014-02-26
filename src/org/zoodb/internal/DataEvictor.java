/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import java.lang.reflect.Field;
import java.util.ArrayList;

import org.zoodb.api.impl.ZooPCImpl;
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
	 * @param def
	 * @param evictPrimitives
	 */
	public DataEvictor(ZooClassDef def, boolean evictPrimitives) {
		ArrayList<Field> rfl = new ArrayList<Field>();
		ArrayList<ZooFieldDef> pfl = new ArrayList<ZooFieldDef>();
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
	
	
    public final void evict(ZooPCImpl co) {
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
