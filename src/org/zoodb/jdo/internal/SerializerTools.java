package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBLargeVector;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.internal.DataSerializer.PRIMITIVE;

/**
 * This class contains utility methods for (de-)serialization.
 *
 * @author Tilmann Zaeschke
 */
class SerializerTools {

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
    static final Map<Class<?>, Integer> preDefClasses; 
    static {
        HashMap<Class<?>, Integer> map = new HashMap<Class<?>, Integer>();
        //primitive classes
        map.put(Boolean.TYPE, PRIMITIVE.BOOL.ordinal());
        map.put(Byte.TYPE, PRIMITIVE.BYTE.ordinal());
        map.put(Character.TYPE, PRIMITIVE.CHAR.ordinal());
        map.put(Double.TYPE, PRIMITIVE.DOUBLE.ordinal());
        map.put(Float.TYPE, PRIMITIVE.FLOAT.ordinal());
        map.put(Integer.TYPE, PRIMITIVE.INT.ordinal());
        map.put(Long.TYPE, PRIMITIVE.LONG.ordinal());
        map.put(Short.TYPE, PRIMITIVE.SHORT.ordinal());
        //primitive array classes
        map.put(boolean[].class, 10);
        map.put(byte[].class, 11);
        map.put(char[].class, 12);
        map.put(double[].class, 13);
        map.put(float[].class, 14);
        map.put(int[].class, 15);
        map.put(long[].class, 16);
        map.put(short[].class, 17);
        
        //primitive classes
        map.put(Boolean.class, 20);
        map.put(Byte.class, 21);
        map.put(Character.class, 22);
        map.put(Double.class, 23);
        map.put(Float.class, 24);
        map.put(Integer.class, 25);
        map.put(Long.class, 26);
        map.put(Short.class, 27);
        //primitive array classes
        map.put(Boolean[].class, 30);
        map.put(Byte[].class, 31);
        map.put(Character[].class, 32);
        map.put(Double[].class, 33);
        map.put(Float[].class, 34);
        map.put(Integer[].class, 35);
        map.put(Long[].class, 36);
        map.put(Short[].class, 37);
        
        //other java classes
        map.put(String.class, 40);
        map.put(String[].class, 41);
        map.put(Date.class, 42);
        map.put(Date[].class, 43);
        map.put(URL.class, 44);
        map.put(URL[].class, 45);
        map.put(UUID.class, 46);
        map.put(UUID[].class, 47);
        
        //persistent classes
        map.put(DBVector.class, 50);
        map.put(DBVector[].class, 51);
        map.put(DBHashtable.class, 52);
        map.put(DBHashtable[].class, 53);
        map.put(DBLargeVector.class, 54);
        map.put(DBLargeVector[].class, 55);
        
        //TODO
        //Map, List, Set, ...?

        preDefClasses = Collections.unmodifiableMap(map);
    }


    
    // Synchronised to allow concurrent access from different Threads.
    private static final Map<Class<?>, List<Field>> _seenClasses = 
        Collections.synchronizedMap(new HashMap<Class<?>, List<Field>>());

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
