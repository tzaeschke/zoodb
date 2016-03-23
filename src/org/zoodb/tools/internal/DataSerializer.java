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
package org.zoodb.tools.internal;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBHashMap;
import org.zoodb.api.DBHashSet;
import org.zoodb.api.DBLargeVector;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.SerializerTools.PRIMITIVE;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Util;


/**
 * This class is basically a copy of the internal DataSerializer in ZooDB. The intention of this
 * code duplication is to provide stability, i.e. while the internal serializer may evolve,
 * the XML serializer should stay stable.
 * This is especially important because the XML import/export should work as a last resort 
 * workaround to migrate data between ZooDB versions, including cases where the internal
 * DeSerializer changed.  
 * 
 * This class serializes objects into a byte stream. For each given object, all
 * non-static and non-transient fields are processed. For all processed fields,
 * references to persistent classes (FCOs) are stored as OID, while references
 * to non-persistent classes (SCOs) are processed in depth.
 * <p>
 * This class is optimized towards following assumptions:<br> 
 * - Type information only needs to be stored for attributes of non-primitive types, because the 
 *   field type can differ from the value type (super type).<br> 
 * - Types are used more than once. So for all types, the name is stored only 
 *   the first time, where the type occurs. For all following occurrences, only 
 *   an id is stored.<br>
 * <p>
 * These optimizations greatly reduce the serialized volume compared to using
 * an ObjectStream, which stores full class definitions for each class. In
 * addition, an ObjectsStream uses 1024 byte blocks for primitives, such
 * bloating the transferred volume even more. <br>
 * Compared to ObjectStream, this class does not require implementation of the
 * Serializable interface. But it requires a default constructor, which can have
 * any modifier of private, protected, public or default.

 * 
 * @author Tilmann Zaeschke
 */
public final class DataSerializer {

    private final XmlWriter out;
    private final ObjectCache cache;

    // Here is how class information is serialized:
    // If the class does not exist in the hashMap, then it is added and its 
    // name is written to the stream. Otherwise only the ID of the class in 
    // the List in written.
    // The class information is required because it can be any sub-type of the
    // Field type, but the exact type is required for instantiation.
    // To make sure that the IDs are the same for
    // serializer and de-serializer, the map has to be rebuild for every Object.
    //--> Should we remove this for normal objects? Maybe, it could save look-ups. --> TODO
    //    -> But we should keep it for DBHashMap/DBArrayList, and also for other serialized
    //       collections.
    private final IdentityHashMap<Class<?>, Byte> usedClasses = 
    	new IdentityHashMap<Class<?>, Byte>();

    /**
     * Instantiate a new DataSerializer.
     * @param out
     */
    public DataSerializer(XmlWriter out, ObjectCache cache) {
        this.out = out;
        this.cache = cache;
    }

    public void writeObject(final GenericObject objectInput, ZooClassDef clsDef) {
        long oid = objectInput.getOid();
        out.startObject(oid);//, objectInput.getClassDef().getOid());

        serializeFieldsGO(objectInput, clsDef);

        // Write special classes
        if (objectInput.isDbCollection()) {
        	serializeSpecial(objectInput, clsDef);
        }
        
        usedClasses.clear();
        
        out.finishObject();
    }

    private final void serializeFieldsGO(GenericObject go, ZooClassDef clsDef) {
        // Write fields
        try {
        	int i = 0;
        	for (ZooFieldDef fd: clsDef.getAllFields()) {
        		out.startField(fd.getFieldPos());
        		if (fd.isPrimitiveType()) {
        			Object v = go.getFieldRaw(i);
                    serializePrimitive(v, fd.getPrimitiveType());
                } else if (fd.isFixedSize()) {
        			Object v = go.getField(fd);
                    serializeObjectNoSCO(v, fd);
                } else {
        			Object v = go.getFieldRawSCO(i);
                    serializeObject(v);
                }
        		i++;
        		out.finishField();
        	}
        } catch (IllegalAccessException e) {
            throw new RuntimeException(getErrorMessage(go), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " + clsDef, e);
        }
    }

    private final void serializeSCO(Object o, Class<?> cls) {
        // Write fields
        try {
            for (Field f : SerializerTools.getFields(cls)) {
                Class<?> type = f.getType();
                if (type.isPrimitive()) {  //This native call is faster than a map-lookup.
                    PRIMITIVE pType = SerializerTools.PRIMITIVE_TYPES.get(type);
                    serializePrimitive(o, f, pType);
                } else {
                    serializeObject(f.get(o));
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(getErrorMessage(o), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " + cls.getName(), e);
        }
    }

    private final void serializeSpecial(GenericObject o, ZooClassDef def) {
    	out.startField(-1);
    	// Perform additional serialization for Persistent Containers
    	if (def.getClassName().equals(DBHashMap.class.getName())) {
    		serializeDBHashMap((HashMap<?, ?>) o.getDbCollection());
    	} else if (def.getClassName().equals(DBLargeVector.class.getName())) {
    		serializeDBList((ArrayList<?>) o.getDbCollection());
    	} else if (def.getClassName().equals(DBArrayList.class.getName())) {
    		serializeDBList((ArrayList<?>) o.getDbCollection());
    	} else if (def.getClassName().equals(DBHashSet.class.getName())) {
    		serializeDBSet((HashSet<?>) o.getDbCollection());
    	}
    	out.finishField();
    }

    private String getErrorMessage(Object o) {
        String msg = "While serializing object: ";
        if (o == null) {
            return msg += "null";
        }
        msg += o.getClass();
        if (o instanceof ZooPC) {
            if (((ZooPC)o).jdoZooIsPersistent()) {
                msg += " OID= " + Util.oidToString(((ZooPC)o).jdoZooGetOid());
            } else {
                msg += " (transient)";
            }
        } else {
            msg += " (not persistent capable)";
        }
        return msg;
    }
    
    private final void serializePrimitive(Object parent, Field field, PRIMITIVE type) 
    		throws IllegalArgumentException, IllegalAccessException {
        // no need to store the type, primitives can't be subclassed.
        switch (type) {
        case BOOLEAN: out.writeBoolean(field.getBoolean(parent)); break;
        case BYTE: out.writeByte(field.getByte(parent)); break;
        case CHAR: out.writeChar(field.getChar(parent)); break;
        case DOUBLE: out.writeDouble(field.getDouble(parent)); break;
        case FLOAT: out.writeFloat(field.getFloat(parent)); break;
        case INT: out.writeInt(field.getInt(parent)); break;
        case LONG: out.writeLong(field.getLong(parent)); break;
        case SHORT: out.writeShort(field.getShort(parent)); break;
        }
    }

    
    private final void serializePrimitive(Object v, PRIMITIVE type) 
    		throws IllegalArgumentException, IllegalAccessException {
        // no need to store the type, primitives can't be subclassed.
        switch (type) {
        case BOOLEAN: out.writeBoolean((Boolean) v); break;
        case BYTE: out.writeByte((Byte) v); break;
        case CHAR: out.writeChar((Character) v); break;
        case DOUBLE: out.writeDouble((Double) v); break;
        case FLOAT: out.writeFloat((Float) v); break;
        case INT: out.writeInt((Integer) v); break;
        case LONG: out.writeLong((Long) v); break;
        case SHORT: out.writeShort((Short) v); break;
        }
    }

    
    /**
     * Method for serializing data with constant size so that it can be stored in the object header
     * where the field offsets are valid.
     */
    private final void serializeObjectNoSCO(Object v, ZooFieldDef def) {
        // Write class/null info
        if (v == null) {
            writeClassInfo(null, null);
            return;
        }
        
        //Persistent capable objects do not need to be serialized here.
        //If they should be serialized, then it will happen in serializeFields()
        Class<? extends Object> cls = v.getClass();
        writeClassInfo(cls, v);

        if (isPersistentCapable(cls)) {
            serializeOid(v);
            return;
        } else if (String.class == cls) {
        	String s = (String)v; 
        	out.writeString(s);
            return;
        } else if (Date.class == cls) {
            out.writeLong(((Date) v).getTime());
            return;
        } else if (GenericObject.class == cls) {
        	serializeOidGo((GenericObject) v);
        	return;
        }
        
        throw new IllegalArgumentException("Illegal class: " + cls + " from " + def);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private final void serializeObject(Object v) {
        // Write class/null info
        if (v == null) {
            writeClassInfo(null, null);
            return;
        }
        
        //Persistent capable objects do not need to be serialized here.
        //If they should be serialized, then it will happen in serializeFields()
        Class<? extends Object> cls = v.getClass();
        writeClassInfo(cls, v);

        PRIMITIVE prim = SerializerTools.PRIMITIVE_CLASSES.get(cls);
        if (prim != null) {
            serializeNumber(v, prim);
            return;
        } else if (String.class == cls) {
            writeString((String) v);
            return;
        } else if (Date.class == cls) {
            out.writeLong(((Date) v).getTime());
            return;
        } else if (isPersistentCapable(cls)) {
            serializeOid(v);
            return;
        } else if (GenericObject.class == cls) {
            serializeOidGo((GenericObject)v);
            return;
        } else if (cls.isArray()) {
            serializeArray(v);
            return;
        } else if (cls.isEnum()) {
        	serializeEnum(v);
        	return;
        }

        // Check Map, this includes Hashtable, they are treated separately from
        // other collections to improve performance and to enforce serialization
        // of keys, because their hash-code is needed for de-serialization.
        if (Map.class.isAssignableFrom(cls)) {
            Map m = (Map) v;
            out.writeInt(m.size());
            for (Map.Entry e: (Set<Map.Entry>)m.entrySet()) {
                serializeObject(e.getKey());
                serializeObject(e.getValue());
            }
            return;
        }

        //Check Set, they are treated separately from other collections to 
        //enforce serialization of key-objects including hash-code.
        if (Set.class.isAssignableFrom(cls)) {
            Set<?> m = (Set<?>) v;
            out.writeInt(m.size());
            for (Object e: m) {
                serializeObject(e);
            }
            return;
        }

        // Check Collection, this includes List, Vector
        if (Collection.class.isAssignableFrom(cls)) {
            Collection<?> l = (Collection<?>) v;
            // ordered
            out.writeInt(l.size());
            for (Object e : l) {
                serializeObject(e);
            }
            return;
        }

        // TODO disallow? Allow Serializable/ Externalizable
        System.out.println("Serializing SCO: " + cls.getName());
        serializeSCO(v, cls);
    }

    private final void serializeNumber(Object v, PRIMITIVE prim) {
        switch (prim) {
        case BOOLEAN: out.writeBoolean((Boolean) v); break;
        case BYTE: out.writeByte((Byte) v); break;
        case CHAR: out.writeChar((Character) v); break;
        case DOUBLE: out.writeDouble((Double) v); break;
        case FLOAT: out.writeFloat((Float) v); break;
        case INT: out.writeInt((Integer) v); break;
        case LONG: out.writeLong((Long) v); break;
        case SHORT: out.writeShort((Short) v); break;
        }
    }

    private final void serializeEnum(Object v) {
    	Class<?> cls = v.getClass();
        writeClassInfo(cls, v);
        out.writeShort((short)((Enum<?>)v).ordinal());
    }

    private final void serializeArray(Object v) {

        //  write component type and dimensions
        
        //write long version (e.g. 'boolean');
        Class<?> innerCompType = getComponentType(v);
        writeClassInfo(innerCompType, null);

        //write short version (e.g. 'Z')
        String compTypeShort = v.getClass().getName();
        int dims = compTypeShort.lastIndexOf('[') + 1;
        compTypeShort = compTypeShort.substring(dims);
        writeString(compTypeShort);
        
        // write dimensions
        out.writeShort((short) dims);

        // serialize array data
        serializeColumn(v, innerCompType, innerCompType.isPrimitive());
    }

    private final void serializeColumn(Object array, Class<?> compType, boolean isPrimitive) {

        //write length or -1 for 'null'
        if (array == null) {
            out.writeInt(-1);
            return;
        }
        int l = Array.getLength(array);
        out.writeInt(l);

        //In case of multi-dimensional arrays, write inner arrays. 
        if (array.getClass().getName().charAt(1) == '[') {
            // multi-dimensional array
            for (int i = 0; i < l; i++) {
                Object o = Array.get(array, i);
                serializeColumn(o, compType, isPrimitive);
            }
            return;
        }
        
        // One dimensional array
        // Now serialize the actual values
        if (isPrimitive) {
            if (compType == Boolean.TYPE) {
                boolean[] a = (boolean[]) array;
                for (int i = 0; i < l; i++) {
                    out.writeBoolean(a[i]);
                }
            } else if (compType == Byte.TYPE) {
                out.write((byte[]) array);
            } else if (compType == Character.TYPE) {
                char[] a = (char[]) array;
                for (int i = 0; i < l; i++) {
                    out.writeChar(a[i]);
                }
            } else if (compType == Float.TYPE) {
                float[] a = (float[]) array;
                for (int i = 0; i < l; i++) {
                    out.writeFloat(a[i]);
                }
            } else if (compType == Double.TYPE) {
                double[] a = (double[]) array;
                for (int i = 0; i < l; i++) {
                    out.writeDouble(a[i]);
                }
            } else if (compType == Integer.TYPE) {
                int[] a = (int[]) array;
                for (int i = 0; i < l; i++) {
                    out.writeInt(a[i]);
                }
            } else if (compType == Long.TYPE) {
                long[] a = (long[]) array;
                for (int i = 0; i < l; i++) {
                    out.writeLong(a[i]);
                }
            } else if (compType == Short.TYPE) {
                short[] a = (short[]) array;
                for (int i = 0; i < l; i++) {
                    out.writeShort(a[i]);
                }
            } else {
                throw new UnsupportedOperationException("Unknown type: "
                        + compType);
            }
            return;
        }
        for (int i = 0; i < l; i++) {
            serializeObject(Array.get(array, i));
        }
    }

    /**
     * Get component type of a (multidimensional) array or any other object.
     * 
     * @param object Object (e.g. a multidimensional array).
     * @return Component type (e.g. int, Boolean.class, String.class, double,
     *         etc.).
     */
    public static final Class<?> getComponentType(Object object) {
        Class<?> result = object.getClass().getComponentType();
        while (result.isArray()) {
            result = result.getComponentType();
        }
        return result;
    }

    private final void serializeDBHashMap(HashMap<?, ?> l) {
        // This class is treated separately, because the links to
        // the contained objects don't show up via reflection API.
    	out.writeInt(l.size());
        for (Map.Entry<?, ?> e : l.entrySet()) {
            //Enforce serialization of keys to have correct hashcodes here.
            serializeObject(e.getKey());
            serializeObject(e.getValue());
        }
    }

    private final void serializeDBList(ArrayList<?> l) {
        // This class is treated separately, because the links to
        // the contained objects don't show up via reflection API.
        out.writeInt(l.size());
        for (Object e : l) {
            serializeObject(e);
        }
    }

    private final void serializeDBSet(HashSet<?> s) {
        // This class is treated separately, because the links to
        // the contained objects don't show up via reflection API.
        out.writeInt(s.size());
        for (Object e : s) {
            serializeObject(e);
        }
    }

    private final void serializeOid(Object obj) {
        out.writeLong(((ZooPC)obj).jdoZooGetOid());
    }

    private final void serializeOidGo(GenericObject obj) {
        out.writeLong(obj.getOid());
    }

    private final void writeClassInfo(Class<?> cls, Object val) {
        if (cls == null) {
            out.writeByte((byte) -1); // -1 for null-reference
            return;
        }

        Byte id = SerializerTools.PRE_DEF_CLASSES_MAP.get(cls);
        if (id != null) {
            // write ID
            out.writeByte(id);
            return;
        }
        
        //for persistent classes, store oid of schema. Fetching it should be fast, it should
        //be in the local cache.
        if (isPersistentCapable(cls)) {
            out.writeByte(SerializerTools.REF_PERS_ID);
            if (val != null) {
            	long soid = ((ZooPC)val).jdoZooGetClassDef().getOid();
            	out.writeLong(soid);
            } else {
            	long soid = cache.getSchema(cls).getOid();
            	out.writeLong(soid);
            }
            return;
        }
        if (GenericObject.class == cls) {
            out.writeByte(SerializerTools.REF_PERS_ID);
            if (val != null) {
            	long soid = ((GenericObject)val).jdoZooGetClassDef().getOid();
            	out.writeLong(soid);
            } else {
            	long soid = cache.getSchema(cls).getOid();
            	out.writeLong(soid);
            }
            return;
        }
        
        if (cls.isArray()) {
            out.writeByte(SerializerTools.REF_ARRAY_ID);
            return;
        }
        
        //did we have this class before?
        id = usedClasses.get(cls);
        if (id != null) {
            // write ID
            out.writeByte(id);
            return;
        }

        // new class
        // write class name
        out.writeByte((byte) 0); // 0 for unknown class id
        writeString(cls.getName());
        //ofs+1 for 1st class, ...
        int idInt = (usedClasses.size() + 1 + SerializerTools.REF_CLS_OFS);
        if (idInt > 125) {
        	//TODO improve encoding to allow 250 classes. Maybe allow negative IDs (-127<id<-1)?
        	throw DBLogger.newFatalInternal("Too many SCO types: " + idInt);
        }
        usedClasses.put(cls, (byte)idInt); 
    }

    private final void writeString(String s) {
    	out.writeString(s);
    }

    static final boolean isPersistentCapable(Class<?> cls) {
        return ZooPC.class.isAssignableFrom(cls);
    }
}
