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
            for (ZooFieldDef fd: co.getClassDef().getAllFields()) {
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
