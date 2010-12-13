package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBLargeVector;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * This class contains utility methods for (de-)serialization.
 *
 * @author Tilmann Zaeschke
 */
class SerializerTools {

    // ********************** persistent types enums *******************************

    
    //Avoiding 'if' cascades reduced time in e.g. serializePrimitive by 25% 
    enum PRIMITIVE {
        /** BOOL */ BOOL, 
        /** BYTE */ BYTE, 
        /** CHAR */ CHAR, 
        /** DOUBLE */ DOUBLE, 
        /** FLOAT */ FLOAT, 
        /** INT */ INT, 
        /** LONG */ LONG,
        /** SHORT */ SHORT}
    static final IdentityHashMap<Class<?>, PRIMITIVE> PRIMITIVE_CLASSES = 
        new IdentityHashMap<Class<?>, PRIMITIVE>();
    static {
        PRIMITIVE_CLASSES.put(Boolean.class, PRIMITIVE.BOOL);
        PRIMITIVE_CLASSES.put(Byte.class, PRIMITIVE.BYTE);
        PRIMITIVE_CLASSES.put(Character.class, PRIMITIVE.CHAR);
        PRIMITIVE_CLASSES.put(Double.class, PRIMITIVE.DOUBLE);
        PRIMITIVE_CLASSES.put(Float.class, PRIMITIVE.FLOAT);
        PRIMITIVE_CLASSES.put(Integer.class, PRIMITIVE.INT);
        PRIMITIVE_CLASSES.put(Long.class, PRIMITIVE.LONG);
        PRIMITIVE_CLASSES.put(Short.class, PRIMITIVE.SHORT);
    }

    static final IdentityHashMap<Class<?>, PRIMITIVE> PRIMITIVE_TYPES = 
        new IdentityHashMap<Class<?>, PRIMITIVE>();
    static {
        PRIMITIVE_TYPES.put(Boolean.TYPE, PRIMITIVE.BOOL);
        PRIMITIVE_TYPES.put(Byte.TYPE, PRIMITIVE.BYTE);
        PRIMITIVE_TYPES.put(Character.TYPE, PRIMITIVE.CHAR);
        PRIMITIVE_TYPES.put(Double.TYPE, PRIMITIVE.DOUBLE);
        PRIMITIVE_TYPES.put(Float.TYPE, PRIMITIVE.FLOAT);
        PRIMITIVE_TYPES.put(Integer.TYPE, PRIMITIVE.INT);
        PRIMITIVE_TYPES.put(Long.TYPE, PRIMITIVE.LONG);
        PRIMITIVE_TYPES.put(Short.TYPE, PRIMITIVE.SHORT);
    }


    // ********************** persistent classes dictionary *******************************
    
    
    private static final class RefDummy {};
    private static final class RefNull {};
    private static final class RefPersistent {};
    //dummy is used, because 0 means undefined class
    public static final Class<?> REF_DUMMY = RefDummy.class; 
    public static final Class<?> REF_NULL = RefNull.class; 
    public static final Class<?> REF_PERS = RefPersistent.class;
    public static final short REF_PERS_ID;
    public static final int REF_CLS_OFS;
    
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
    static final IdentityHashMap<Class<?>, Short> PRE_DEF_CLASSES_MAP;
    static final ArrayList<Class<?>> PRE_DEF_CLASSES_ARRAY; 
    static {
        IdentityHashMap<Class<?>, Short> map = new IdentityHashMap<Class<?>, Short>();
        ArrayList<Class<?>> list = new ArrayList<Class<?>>();
        list.add(REF_DUMMY);
        list.add(REF_PERS);
        list.add(Object.class);
        list.add(PersistenceCapableImpl.class);
        
        //primitive classes
        list.add(Boolean.TYPE);
        list.add(Byte.TYPE);
        list.add(Character.TYPE);
        list.add(Double.TYPE);
        list.add(Float.TYPE);
        list.add(Integer.TYPE);
        list.add(Long.TYPE);
        list.add(Short.TYPE);
        
        //primitive array classes
        list.add(boolean[].class);
        list.add(byte[].class);
        list.add(char[].class);
        list.add(double[].class);
        list.add(float[].class);
        list.add(int[].class);
        list.add(long[].class);
        list.add(short[].class);
        
        //primitive classes
        list.add(Boolean.class);
        list.add(Byte.class);
        list.add(Character.class);
        list.add(Double.class);
        list.add(Float.class);
        list.add(Integer.class);
        list.add(Long.class);
        list.add(Short.class);
        
        //primitive array classes
        list.add(Boolean[].class);
        list.add(Byte[].class);
        list.add(Character[].class);
        list.add(Double[].class);
        list.add(Float[].class);
        list.add(Integer[].class);
        list.add(Long[].class);
        list.add(Short[].class);
        
        //other java classes
        list.add(String.class);
        list.add(String[].class);
        list.add(Date.class);
        list.add(Date[].class);
        list.add(URL.class);
        list.add(URL[].class);
        list.add(UUID.class);
        list.add(UUID[].class);
        list.add(Enum.class);
        
        //persistent classes
        list.add(DBVector.class);
        list.add(DBVector[].class);
        list.add(DBHashtable.class);
        list.add(DBHashtable[].class);
        list.add(DBLargeVector.class);
        list.add(DBLargeVector[].class);
        
        //Map, List, Set, ...?
//        list.add(HashMap.class);
//        list.add(HashSet.class);
//        list.add(LinkedList.class);
//        list.add(ArrayList.class);
//        list.add(Hashtable.class);
//        list.add(Vector.class);

        
        //fill map
        for (int i = 0; i < list.size(); i++) {
            map.put(list.get(i), (short)i);
        }
        
        //TODO use real array?
        //PRE_DEF_CLASSES_ARRAY = Collections.unmodifiableList(list);
        PRE_DEF_CLASSES_ARRAY = list;
        //PRE_DEF_CLASSES_MAP = (IdentityHashMap<Class<?>, Short>) Collections.unmodifiableMap(map);
        PRE_DEF_CLASSES_MAP = map;
        
        //TODO set fixed, e.g. 100?
        REF_CLS_OFS = list.size();
        REF_PERS_ID = map.get(REF_PERS);
    }


    
    // ********************** persistent classes fields *******************************
    
    
    
    // Synchronised to allow concurrent access from different Threads.
    private static final ConcurrentHashMap<Class<?>, List<Field>> _seenClasses = 
        new ConcurrentHashMap<Class<?>, List<Field>>();

    static final List<Field> getFields(Class<?> cl) {
        List<Field> fields = _seenClasses.get(cl);
        if (fields != null) {
            return _seenClasses.get(cl);
        }

        // TODO the following could be optimised:
        // Instead of filling a List with all attributes and then removing the
        // undesired ones, the List should be build up only with desired
        // attributes
        // while looping through the individual arrays from getDeclaredFields();

        // Add attributes of super-classes for jar,
        // these are already included in the database class.
        Field[] fieldA = cl.getDeclaredFields();
        fields = new LinkedList<Field>(Arrays.asList(fieldA));
        Class<?> cSuper = cl.getSuperclass();
        while (cSuper != null) {
            Collections.addAll(fields, cSuper.getDeclaredFields());
            cSuper = cSuper.getSuperclass();
        }

        // Remove transient and static attrs from list (they are not in the DB)
        ListIterator<Field> vil = fields.listIterator();
        vil = fields.listIterator(0);
        int modifiers;
        while (vil.hasNext()) {
            try {
                Field field = vil.next();
                modifiers = field.getModifiers();
                if (Modifier.isTransient(modifiers)
                        || Modifier.isStatic(modifiers)) {
                    vil.remove();
                    continue;
                }

                if (field.getName().startsWith("_vj_")) {
                    throw new IllegalStateException("Bad field: " + field);
                }

                // The following is alright because this class is only used
                // in a daemon.
                field.setAccessible(true);
            } catch (RuntimeException e) {
                throw e;
            }
        }
        //Because we are not in a synchronised block, this may actually
        //overwrite an existing entry. But it's very unlikely and would not do
        //any harm, so we do not synchronise here. Using a synchronised Map
        //should be sufficient.
        _seenClasses.put(cl, fields);

        return fields;
    }
}
