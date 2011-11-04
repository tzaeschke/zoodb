/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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

import org.zoodb.jdo.internal.SerializerTools.PRIMITIVE;
import org.zoodb.jdo.spi.PersistenceCapableImpl;


/**
 * This class provides a method to nullify objects (= evicting an object) 
 * 
 * @author Tilmann Zaeschke
 */
public final class DataEvictor {

    public static final void nullify(PersistenceCapableImpl co) {
        try {
            //set fields
            for (ZooFieldDef fd: co.jdoZooGetClassDef().getAllFields()) {
                Field f = fd.getJavaField();
                PRIMITIVE prim = fd.getPrimitiveType();
                if (prim != null) {
                    deserializePrimitive(co, f, prim);
                } else {
                    f.set(co, null);
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final void deserializePrimitive(Object parent, Field field, PRIMITIVE prim) 
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
