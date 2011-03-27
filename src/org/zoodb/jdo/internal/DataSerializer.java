package org.zoodb.jdo.internal;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOObjectNotFoundException;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBLargeVector;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.spi.PersistenceCapableImpl;


/**
 * This class serializes objects into a byte stream. For each given object, all
 * non-static and non-transient fields are processed. For all processed fields,
 * references to persistent classes (FCOs) are stored as LOID, while references
 * to non-persistent classes (SCOs) are processed in depth.
 * <p>
 * This class is optimized towards following assumptions:<br> 
 * - The class definitions and ordering of fields is the same on the client 
 * and the server. This is superficially checked via a CRC32 checksum of the 
 * _usedClasses Map. This assumption avoids the need to transfer full class 
 * definitions (including field ordering). Type information only needs to be 
 * stored for attributes of non-primitive types, because the field type can 
 * differ from the value type (super type).<br> 
 * - Types are used more than once. So for all types, the name is stored only 
 * the first time, where the type occurs. For all following occurrences, only 
 * an id is stored.<br>
 * <p>
 * These optimizations greatly reduce the transferred volume compared to using
 * an ObjectStream, which stores full class definitions for each class. In
 * addition, an ObjectsStream uses 1024 byte blocks for primitives, such
 * bloating the transferred volume even more. <br>
 * Compared to ObjectStream, this class does not require implementation of the
 * Serialisable interface. But it requires a default constructor, which can have
 * any modifier of private, protected, public or default.
 * 
 * TODO improve performance: E.g. garbage collection
 * http://java.sun.com/docs/hotspot/
 * http://java.sun.com/docs/books/performance/1st_edition/html/JPAppHotspot.fm.html#1006054
 * 
 * @author Tilmann Zaeschke
 */
public final class DataSerializer {

    private final SerialOutput _out;
    private final AbstractCache _cache;
    private final Node _node;

    // Here is how class information is transmitted:
    // If the class does not exist in the hashMap, then it is added and its 
    // name is written to the stream. Otherwise only the id of the class in 
    // the List in written.
    // The class information is required because it can be any sub-type of the
    // Field type, but the exact type is required for instantiation.
    // This can't be static. To make sure that the IDs are the same for
    // server and client, the map has to be rebuild for every Transaction,
    //or at least for every new connection.
    // Otherwise problems would occur if e.g. one of the processes crash
    // and has to rebuild it's map, or if the Sender uses the same Map for
    // all receivers, regardless whether they all get the same data.
    private IdentityHashMap<Class<?>, Byte> _usedClasses = new IdentityHashMap<Class<?>, Byte>();

    /**
     * Instantiate a new DataSerializer.
     * @param out
     * @param filter
     */
    public DataSerializer(SerialOutput out, AbstractCache cache, Node node) {
        _out = out;
        _cache = cache;
        _node = node;
    }

    /**
     * Writes all objects in the List to the output stream. This requires all
     * objects to have persistent state.
     * <p>
     * All the objects need to be FCOs. All their referenced SCOs are serialised
     * as well. References to FCOs are substituted by OIDs.
     * 
     * @param objectsInput
     */
    public void writeObjects(final Collection<PersistenceCapableImpl> objectsInput, 
    		ZooClassDef clsDef) {
        Set<Object> objects = new ObjectIdentitySet<Object>();
        for (Object obj: objectsInput) {
            addKeysForHashing(objects, obj);
        }
        
        // write header
        _out.writeInt(objects.size());
        for (Object obj : objects) {
            writeObjectHeader(obj, clsDef);
        }

        // Send object bodies
        for (Object obj : objects) {
            serializeFields(obj, obj.getClass());
        }
    }

    /**
     * Writes all objects in the List to the output stream. This requires all
     * objects to have persistent state.
     * <p>
     * All the objects need to be FCOs. All their referenced SCOs are serialized
     * as well. References to FCOs are substituted by OIDs.
     * 
     * @param objectInput
     * @param clsDef 
     */
    public void writeObject(final Object objectInput, ZooClassDef clsDef) {
        // write header
        writeObjectHeader(objectInput, clsDef);

        Set<Object> objects = new ObjectIdentitySet<Object>();
        objects.add(objectInput);
        if (objectInput instanceof Map || objectInput instanceof Set) {
            addKeysForHashing(objects, objectInput);
	        _out.writeInt(objects.size()-1);
	        for (Object obj : objects) {
	        	if (obj != objectInput)
	        		//TODO, this is the wrong class,
	        		writeObjectHeader(obj, clsDef);
	        }
        }

        // Send object bodies
        for (Object obj : objects) {
            serializeFields(obj, obj.getClass());
        }
    }

    /**
     * We have to serialize all persistent keys in this transaction.
     * We need to make sure that keys of Sets and Maps are already present in the cache in the same 
     * transaction. Otherwise they may be stored with the wrong hash codes. 
     * @param objects
     * @param obj
     */
    private void addKeysForHashing(Set<Object> objects, Object obj) {
        if (obj instanceof Set) {
            for (Object key: (Set)obj) {
                addKeysForHashing(objects, key);
            }
        } else if (obj instanceof Map) {
            for (Object key: ((Map)obj).keySet()) {
                addKeysForHashing(objects, key);
            }
        }
        if (!isPersistentCapable(obj.getClass())) {
            return;
        }
        objects.add(obj);
    }

    private final void writeObjectHeader(Object obj, ZooClassDef clsDef) {
        // write class info
    	_out.writeLong(clsDef.getOid());
        Class<?> cls = obj.getClass();

        // Write LOID
        serializeLoid(obj);

        if (DBHashtable.class.isAssignableFrom(cls)) {
            _out.writeInt(((DBHashtable<?, ?>)obj).size());
        } else if (DBVector.class.isAssignableFrom(cls)) {
            _out.writeInt(((DBVector<?>)obj).size());
        }
    }

    private final void serializeFields(Object o, Class<?> cls) {
        // Write fields
        try {
            for (Field f : SerializerTools.getFields(cls)) {
                Class<?> type = f.getType();
                if (SerializerTools.PRIMITIVE_TYPES.containsKey(type)) {
                    serializePrimitive(o, f, type);
                } else {
                    serializeObject(f.get(o));
                }
            }

            // Perform additional serialisation for Persistent Containers
            if (DBHashtable.class.isAssignableFrom(cls)) {
                serializeDBHashtable((DBHashtable<?, ?>) o);
            } else if (DBLargeVector.class.isAssignableFrom(cls)) {
                serializeDBLargeVector((DBLargeVector) o);
            } else if (DBVector.class.isAssignableFrom(cls)) {
                serializeDBVector((DBVector<?>) o);
            }
        } catch (JDOObjectNotFoundException e) {
        	throw new RuntimeException(getErrorMessage(o), e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(getErrorMessage(o), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(getErrorMessage(o), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " + cls.getName(), e);
        }
    }

    private String getErrorMessage(Object o) {
        String msg = "While serializing object: ";
        if (o == null) {
            return msg += "null";
        }
        msg += o.getClass();
        if (o instanceof PersistenceCapableImpl) {
            if (((PersistenceCapableImpl)o).jdoIsPersistent()) {
                msg += " LOID= " + Util.oidToString(((PersistenceCapableImpl)o).jdoGetObjectId());
            } else {
                msg += " (transient)";
            }
        } else {
            msg += " (not persistent capable)";
        }
        return msg;
    }
    
    private final void serializePrimitive(Object parent, Field field, Class<?> type) 
    		throws IllegalArgumentException, IllegalAccessException {
        // no need to store the type, primitives can't be subclassed.
        switch (SerializerTools.PRIMITIVE_TYPES.get(type)) {
        case BOOL: _out.writeBoolean(field.getBoolean(parent)); break;
        case BYTE: _out.writeByte(field.getByte(parent)); break;
        case CHAR: _out.writeChar(field.getChar(parent)); break;
        case DOUBLE: _out.writeDouble(field.getDouble(parent)); break;
        case FLOAT: _out.writeFloat(field.getFloat(parent)); break;
        case INT: _out.writeInt(field.getInt(parent)); break;
        case LONG: _out.writeLong(field.getLong(parent)); break;
        case SHORT: _out.writeShort(field.getShort(parent)); break;
        }
    }

    /**
     * 
     * @param v Object to be serialized.
     * @param forced The forced flag indicates that the object should be 
     * serialized, even if it is a persistent object. This is useful when
     * serializing the keys of Maps and Sets.
     * @throws IOException
     */
    private final void serializeObject(Object v) {
        // Write class/null info
        if (v == null) {
            writeClassInfo(null);
            return;
        }
        
        //Persistent capable objects do not need to be serialised here.
        //If they should be serialised, then it will happen in serializeFields()
        Class<? extends Object> cls = v.getClass();
        writeClassInfo(cls);

        if (SerializerTools.PRIMITIVE_CLASSES.containsKey(cls)) {
            serializeNumber(v, cls);
            return;
        } else if (String.class == cls) {
            writeString((String) v);
            return;
        } else if (Date.class == cls) {
            _out.writeLong(((Date) v).getTime());
            return;
        } else if (isPersistentCapable(cls)) {
            serializeLoid(v);
            return;
        } else if (cls.isArray()) {
            serializeArray(v);
            return;
        }

        // Check Map, this includes Hashtable, they are treated separately from
        // other collections to improve performance and to enforce serialization
        // of keys, because their hash-code is needed for de-serialization  
        // (SPR 5475).
        if (Map.class.isAssignableFrom(cls)) {
            Map m = (Map) v;
            _out.writeInt(m.size());
            for (Map.Entry e: (Set<Map.Entry>)m.entrySet()) {
                serializeObject(e.getKey());
                serializeObject(e.getValue());
            }
            return;
        }

        //Check Set, they are treated separately from other collections to 
        //enforce serialization of key-objects including hash-code (SPR 5475).
        if (Set.class.isAssignableFrom(cls)) {
            Set<?> m = (Set<?>) v;
            _out.writeInt(m.size());
            for (Object e: m) {
                serializeObject(e);
            }
            return;
        }

        // Check Collection, this includes List, Vector
        if (Collection.class.isAssignableFrom(cls)) {
            Collection<?> l = (Collection<?>) v;
            // ordered
            _out.writeInt(l.size());
            for (Object e : l) {
                serializeObject(e);
            }
            return;
        }

        // TODO disallow? Allow Serializable/ Externalizable
        serializeFields(v, cls);
    }

    private final void serializeNumber(Object v, Class<?> cls) {
        switch (SerializerTools.PRIMITIVE_CLASSES.get(cls)) {
        case BOOL: _out.writeBoolean((Boolean) v); break;
        case BYTE: _out.writeByte((Byte) v); break;
        case CHAR: _out.writeChar((Character) v); break;
        case DOUBLE: _out.writeDouble((Double) v); break;
        case FLOAT: _out.writeFloat((Float) v); break;
        case INT: _out.writeInt((Integer) v); break;
        case LONG: _out.writeLong((Long) v); break;
        case SHORT: _out.writeShort((Short) v); break;
        }
    }

    private final void serializeArray(Object v) {

        //  write component type and dimensions
        
        //write long version (e.g. 'boolean');
        Class<?> innerCompType = getComponentType(v);
        writeClassInfo(innerCompType);

        //write short version (e.g. 'Z')
        String compTypeShort = v.getClass().getName();
        int dims = compTypeShort.lastIndexOf('[') + 1;
        compTypeShort = compTypeShort.substring(dims);
        writeString(compTypeShort);
        
        // write dimensions
        _out.writeShort((short) dims);

        // serialize array data
        serializeColumn(v, innerCompType, innerCompType.isPrimitive());
    }

    private final void serializeColumn(Object array, Class<?> compType, boolean isPrimitive) {

        //write length or -1 for 'null'
        if (array == null) {
            _out.writeInt(-1);
            return;
        }
        int l = Array.getLength(array);
        _out.writeInt(l);

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
                    _out.writeBoolean(a[i]);
                }
            } else if (compType == Byte.TYPE) {
                _out.write((byte[]) array);
            } else if (compType == Character.TYPE) {
                char[] a = (char[]) array;
                for (int i = 0; i < l; i++) {
                    _out.writeChar(a[i]);
                }
            } else if (compType == Float.TYPE) {
                float[] a = (float[]) array;
                for (int i = 0; i < l; i++) {
                    _out.writeFloat(a[i]);
                }
            } else if (compType == Double.TYPE) {
                double[] a = (double[]) array;
                for (int i = 0; i < l; i++) {
                    _out.writeDouble(a[i]);
                }
            } else if (compType == Integer.TYPE) {
                int[] a = (int[]) array;
                for (int i = 0; i < l; i++) {
                    _out.writeInt(a[i]);
                }
            } else if (compType == Long.TYPE) {
                long[] a = (long[]) array;
                for (int i = 0; i < l; i++) {
                    _out.writeLong(a[i]);
                }
            } else if (compType == Short.TYPE) {
                short[] a = (short[]) array;
                for (int i = 0; i < l; i++) {
                    _out.writeShort(a[i]);
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

    private final void serializeDBHashtable(DBHashtable<?, ?> l) {
        // This class is treated separately, because the links to
        // the contained objects don't show up via reflection API. TODO
        _out.writeInt(l.size());
        for (Object k : l.keySet()) {
            //Enforce serialization of keys to have correct hashcodes here (SPR 5475)
            serializeObject(k);
            serializeObject(l.get(k));
        }
    }

    private final void serializeDBLargeVector(DBLargeVector l) {
        // This class is treated separately, because the links to
        // the contained objects don't show up via reflection API. TODO 
        _out.writeInt(l.size());
        for (Object e : l) {
            serializeObject(e);
        }
    }

    private final void serializeDBVector(DBVector<?> l) {
        // This class is treated separately, because the links to
        // the contained objects don't show up via reflection API. TODO
        _out.writeInt(l.size());
        for (Object e : l) {
            serializeObject(e);
        }
    }

    private final void serializeLoid(Object obj) {
        _out.writeLong((Long) ((PersistenceCapableImpl)obj).jdoGetObjectId());
    }

    private final void writeClassInfo(Class<?> cls) {
    	//TODO here is how we could save 1 byte:
    	//If field type is sub-type of PersCapableImpl, the just store: -1=null, otherwise soid/oid
    	//If it is not, use the table of SCOs. If the SCO is persitent capable, the  
    	
        if (cls == null) {
            _out.writeByte((byte) -1); // -1 for null-reference
            return;
        }

        Byte id = SerializerTools.PRE_DEF_CLASSES_MAP.get(cls);
        if (id != null) {
            // write ID
            _out.writeByte(id);
            return;
        }
        
        //for persistent classes, store oid of schema. Fetching it should be fast, it should
        //be in the local cache.
        if (isPersistentCapable(cls)) {
            _out.writeByte(SerializerTools.REF_PERS_ID);
            long soid = _cache.getSchema(cls, _node).getOid();
            _out.writeLong(soid);
            return;
        }
        
        //did we have this class before?
        id = _usedClasses.get(cls);
        if (id != null) {
            // write ID
            _out.writeByte(id);
            return;
        }

        // new class
        // write class name
        _out.writeByte((byte) 0); // 0 for unknown class id
        writeString(cls.getName());
        //ofs+1 for 1st class, ...
        int idInt = (_usedClasses.size() + 1 + SerializerTools.REF_CLS_OFS);
        if (idInt > 125) {
        	//TODO improve encoding to allow 250 classes. Maybe allow negative IDs (-127<id<-1)?
        	throw new JDOFatalDataStoreException("Too many SCO type: " + idInt);
        }
        _usedClasses.put(cls, (byte)idInt); 
    }

    private final void writeString(String s) {
    	_out.writeString(s);
    }

    static final boolean isPersistentCapable(Class<?> cls) {
        return PersistenceCapableImpl.class.isAssignableFrom(cls);
    }
}
