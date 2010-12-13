package org.zoodb.jdo.internal;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBLargeVector;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.server.PageAccessFile_BB;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.DatabaseLogger;

/**
 * This class creates instances from a byte stream. All classes that are 
 * processed with this class need to have a default constructor. The 
 * constructor can be all of public, protected, default and private.
 * <p>
 * Treatment of DBParentAware: <br>
 * There are some serious problems with classes that are DBParentAware. They
 * are causing a vicious circle:<br>
 * 1) Full-copy requires the ability to set fields of objects to reference 
 * other objects that have not yet been copied. This this is solved via Dummy 
 * objects. <br>
 * 2) At the point of making the object persistent, we need to know the name
 * of the target database. This name is derived via the DestinationRegistry. 
 * <br>
 * 3) For DBParentAware, the database name depends on the parent object.
 * The parent object cannot be guaranteed to exist (copied earlier, or in 
 * same tx). <br>
 * 4) So to make a DBHashtable persistent, we require the parent object to be
 * persistent, which is difficult to archive. It can be impossible in case the
 * parent is e.g. DBVector, which is copied later. <br>
 * Solution: For DBParentAware, we copy the ClassID of the parent. That is not 
 * the immediate parent, but the one that is not DBParentAware itself.<br>
 * This requires all Destinations (except DBParentAware) to be class based.
 * 
 * 
 * @author Tilmann Zaeschke
 */
public class DataDeSerializer {

    private SerialInput _in;
    boolean _zipped = false;
    
    //Here is how class information is transmitted:
    //If the class does not exist in the hashMap, then it is added and its 
    //name is written to the stream. Otherwise only the id of the class in the 
    //List in written.
    //The class information is required because it can be any sub-type of the
    //Field type, but the exact type is required for instantiation.
    private ArrayList<Class<?>> _usedClasses = new ArrayList<Class<?>>(20);
    
    private static Map<Class<?>, Constructor<?>> _defaultConstructors = 
        Collections.synchronizedMap(new HashMap<Class<?>, Constructor<?>>(100));
    private static Map<Class<?>, Constructor<?>> _sizedConstructors = 
        Collections.synchronizedMap(new HashMap<Class<?>, Constructor<?>>(10));
    
    private AbstractCache _cache;
    private Node _node;
    
    //Cached Sets and Maps
    //The maps and sets are only filled after the keys have been de-serialized. Otherwise 
    //the keys will be inserted with a wrong hash value.
    //TODO load MAPS and SETS in one go and load all keys right away!
    //TODO or do not use add functionality, but serialize internal arrays right away! Probably does
    //not work for mixtures of LinkedLists and set like black-whit tree. (?).
    private List<MapValuePair> _mapsToFill = new ArrayList<MapValuePair>();
    private List<SetValuePair> _setsToFill = new ArrayList<SetValuePair>();
    private static class MapEntry { 
        Object K; Object V; 
        public MapEntry(Object key, Object value) {
            K = key;
            V = value;
        }
    }
    private static class MapValuePair { 
        Map<Object, Object> _map; List<MapEntry> _values; 
        public MapValuePair(Map<Object, Object> map, List<MapEntry> values) {
            _map = map;
            _values = values;
        }
    }
    private static class SetValuePair { 
        Set<Object> _set; List<Object> _values; 
        public SetValuePair(Set<Object> set, List<Object> values) {
            _set = set;
            _values = values;
        }
    }
    
    /**
     * Create a new DataDeserializer.
     * @param in Stream to read the data from.
     * persistent.
     */
    public DataDeSerializer(SerialInput in, 
    		AbstractCache cache, Node node) {
        _in = in;
        _cache = cache;
        _node = node;
    }
        
    /**
     * This method returns a List of objects that are read from the input 
     * stream. The returned objects have not been made persistent.
     * @return List of read objects.
     * @throws IOException 
     */
    public Set<PersistenceCapableImpl> readObjects() throws IOException {
        //Read object header. This allows pre-initialisation of object,
        //which is helpful in case a later object is referenced by an 
        //earlier one.
        int nH = _in.readInt();
        
        //We need to maintain two collections here:
        //- preLoaded contains objects from the serialized stream, in 
        //  A) correct order and
        //  B) allowing multiples (even though this should not happen)
        //  This is required to process the second loop (read fields).
        //- _deserialisedObjects to find duplicates (should not happen) and
        //  avoid multiple objects with the same LOID in the cache. This 
        //  collection is also used to prevent unnecessary creation of dummies
        //  that have deserialised pendants.
        Set<PersistenceCapableImpl> preLoaded = new LinkedHashSet<PersistenceCapableImpl>(nH);
        _setsToFill = new ArrayList<SetValuePair>();
        _mapsToFill = new ArrayList<MapValuePair>();
        
        //Add the cached Objects to the list of '_cachedObjects' objects.
//        for (Object id: cachedObjIDs) {
//        	//TODO rem            _cachedObjects.add((Long)id);
//        }
        //TODO add to real cache?
        
        for (int i = 0; i < nH; i++) {
            PersistenceCapableImpl obj = readPersistentObjectHeader();
            preLoaded.add(obj);
        }

        //read objects data
        int i = 1;
        for (PersistenceCapableImpl obj: preLoaded) {
            try {
                deserializeFields( obj, obj.getClass() );
                i++;
            } catch (PropagationCorruptedException e) {
                DatabaseLogger.severe("Corrupted Object ID: " + i + " of " + nH);
                throw e;
            }
        }

        //Rehash collections. SPR 5493. We have to do add all keys again, 
        //because when the collections were first de-serialised, the keys may
        //not have been de-serialised yet (if persistent) therefore their
        //hash-code may have been wrong.
        for (SetValuePair sv: _setsToFill) {
            sv._set.clear();
            for (Object o: sv._values) {
                sv._set.add(o);
            }
        }
        _setsToFill.clear();
        for (MapValuePair mv: _mapsToFill) {
            mv._map.clear();
            for (MapEntry e: mv._values) {
                mv._map.put(e.K, e.V);
            }
        }
        _mapsToFill.clear();
        
        return preLoaded;
    }
    
    private final PersistenceCapableImpl readPersistentObjectHeader() throws IOException {
        //read class info
        Class<?> cls = readClassInfo();
            
        //Read LOID
        long oid = _in.readLong();
    	PersistenceCapableImpl obj = null;
    	CachedObject co = _cache.findCoByOID(oid);
    	if (co != null) {
    		//might be hollow!
    		co.markClean();
    		obj = co.obj;
            //Make the object dirty to ensure that it is written to the 
            //database, even if only a SCO is changed.
            //TODO maybe introduce a group-load before the makeDirty()?
//TODO?            JDOHelper.makeDirty(obj, null);
            if (DBHashtable.class.isAssignableFrom(cls) 
                    || DBVector.class.isAssignableFrom(cls)) {
                _in.readInt();
            }
            return obj;
        }
        
        if (DBHashtable.class.isAssignableFrom(cls)) {
            obj = createSizedInstance(cls, _in.readInt());
            //The class is used to determine the target database.
            prepareObject(obj, oid, false);
        } else if (DBVector.class.isAssignableFrom(cls)) {
            obj = createSizedInstance(cls, _in.readInt());
            prepareObject(obj, oid, false);
        } else {
            obj = (PersistenceCapableImpl) createInstance(cls);
            prepareObject(obj, oid, false);
        }
        return obj;
    }

    @SuppressWarnings("unchecked")
    private final Object deserializeFields(Object obj, Class<?> cls) throws IOException {
        Field f1 = null;
        Object deObj = null;
        try {
            //Read fields
            for (Field field: SerializerTools.getFields(cls)) {
                f1 = field;
                if (!deserializePrimitive(obj, field)) {
                	deObj = deserializeObject();
                    field.set(obj, deObj);
                }
            }

            //Special treatment for persistent containers.
            //Their data is not stored in (visible) fields.
            if (obj instanceof DBHashtable) {
                deserializeDBHashtable((DBHashtable) obj);
            } else if (obj instanceof DBLargeVector) {
                deserializeDBLargeVector((DBLargeVector) obj);
            } else if (obj instanceof DBVector) {
                deserializeDBVector((DBVector) obj);
            }
            return obj;
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (PropagationCorruptedException e) {
            throw new PropagationCorruptedException("Corrupted Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 , e);
        }
    }

    private final boolean deserializePrimitive(Object parent, Field field) 
            throws IOException, IllegalArgumentException, 
            IllegalAccessException {
        DataSerializer.PRIMITIVE prim = 
            DataSerializer.PRIMITIVE_TYPES.get(field.getType());
        if (prim == null) {
            return false;
        }
        switch (prim) {
        case BOOL: field.setBoolean(parent, _in.readBoolean()); break;
        case BYTE: field.setByte(parent, _in.readByte()); break;
        case CHAR: field.setChar(parent, _in.readChar()); break;
        case DOUBLE: field.setDouble(parent, _in.readDouble()); break;
        case FLOAT: field.setFloat(parent, _in.readFloat()); break;
        case INT: field.setInt(parent, _in.readInt()); break;
        case LONG: field.setLong(parent, _in.readLong()); break;
        case SHORT: field.setShort(parent, _in.readShort()); break;
        default:
            throw new UnsupportedOperationException(prim.toString());
        }
        return true;
    }        
             
    /**
     * Serialise objects. If the object is persistent capable, only it's LOID
     * is stored. Otherwise it is serialised on the method is called
     * recursively on all of it's fields.
     * @return Deserialised value.
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private final Object deserializeObject() throws IOException {
        //read class/null info
        Class<?> cls = readClassInfo();
        if (cls == null) {
            //reference is null
            return null;
        }
        
        if (cls.isArray()) {
            return deserializeArray();
        }
        
        //read instance data
        if (isPersistentCapableClass(cls)) {
            long loid = _in.readLong();

            //Is object already in the database or cache?
            Object obj = hollowForOid(loid, cls);
            return obj;
        } else if (DataSerializer.PRIMITIVE_CLASSES.containsKey(cls)) {
            return deserializeNumber(cls);
        } else if (String.class == cls) {
            return deserializeString();
        } else if (Date.class == cls) {
            return new Date(_in.readLong());
        }
        
        if (Map.class.isAssignableFrom(cls)) {
            //ordered 
            int len = _in.readInt();
            Map<Object, Object> m = (Map<Object, Object>) createInstance(cls);  //TODO sized?
            List<MapEntry> values = new ArrayList<MapEntry>(len);
            for (int i=0; i < len; i++) {
                //m.put(deserializeObject(), deserializeObject());
                //We don't fill the Map here, see SPR 5493
                values.add(new MapEntry(deserializeObject(), deserializeObject()));
            }
            _mapsToFill.add(new MapValuePair(m, values));
            return m;
        }
        if (Set.class.isAssignableFrom(cls)) {
            //ordered 
            int len = _in.readInt();
            Set<Object> s = (Set<Object>) createInstance(cls);  //TODO sized?
            List<Object> values = new ArrayList<Object>(len);
            for (int i=0; i < len; i++) {
                //s.add(deserializeObject());
                //We don't fill the Set here, see SPR 5493
                values.add(deserializeObject());
            }
            _setsToFill.add(new SetValuePair(s, values));
            return s;
        }
        //Check Iterable, Map, 'Array'  
        //This would include Vector and Hashtable
        if (Collection.class.isAssignableFrom(cls)) {
            Collection<Object> l = (Collection<Object>) createInstance(cls);  //TODO sized?
            //ordered 
            int len = _in.readInt();
            for (int i=0; i < len; i++) {
                l.add(deserializeObject());
            }
            return l;
        }
        
        // TODO disallow? Allow Serializable/ Externalizable
        return deserializeFields(createInstance(cls), cls);
    }

    private final Object deserializeNumber(Class<?> cls) throws IOException {
        switch (DataSerializer.PRIMITIVE_CLASSES.get(cls)) {
        case BOOL: return _in.readBoolean();
        case BYTE: return _in.readByte();
        case CHAR: return _in.readChar();
        case DOUBLE: return _in.readDouble();
        case FLOAT: return _in.readFloat();
        case INT: return _in.readInt();
        case LONG: return _in.readLong();
        case SHORT: return _in.readShort();
        default: throw new UnsupportedOperationException(
                "Class not supported: " + cls);
        }
    }
    
    private final Object deserializeArray() throws IOException {
        
        // read meta data
        Class<?> innerType = readClassInfo();
        String innerTypeAcronym = deserializeString();
        
        short dims = _in.readShort();
        
        // read data
        return deserializeArrayColumn(innerType, innerTypeAcronym, dims);
    }

    private final Object deserializeArrayColumn(Class<?> innerType, 
            String innerAcronym, int dims) throws IOException {

        //read length
        int l = _in.readInt();
        if (l == -1) {
            return null;
        }
        
        Object array = null;
        
        if (dims > 1) {
            //Create multi-dimensional array
            try {
                char[] ca = new char[dims-1];
                Arrays.fill(ca, '[');
                Class<?> compClass = 
                    Class.forName(new String(ca) + innerAcronym);
                array = Array.newInstance(compClass, l);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < l; i++) {
                Array.set(array, i, 
                        deserializeArrayColumn(innerType, innerAcronym, dims-1)
                );
            }
            return array;
        }

        array = Array.newInstance(innerType, l);

        // deserialise actual content
        if (innerType.isPrimitive()) {
            if (innerType == Boolean.TYPE) {
                boolean[] a = (boolean[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = _in.readBoolean();
                }
            } else if (innerType == Byte.TYPE) {
                _in.readFully((byte[])array);
            } else if (innerType == Character.TYPE) {
                char[] a = (char[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = _in.readChar();
                }
            } else if (innerType == Float.TYPE) {
                float[] a = (float[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = _in.readFloat();
                }
            } else if (innerType == Double.TYPE) {
                double[] a = (double[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = _in.readDouble();
                }
            } else if (innerType == Integer.TYPE) {
                int[] a = (int[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = _in.readInt();
                }
            } else if (innerType == Long.TYPE) {
                long[] a = (long[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = _in.readLong();
                }
            } else if (innerType == Short.TYPE) {
                short[] a = (short[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = _in.readShort();
                }
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported type: " + innerType);
            }
            return array;
        }
        if (Object.class.isAssignableFrom(innerType)) {
            for (int i = 0; i < l; i++) {
                Array.set(array, i, deserializeObject());
            }
            return array;
        }
        throw new UnsupportedOperationException("Unsupported: " + innerType);
    }

    private final void deserializeDBHashtable(DBHashtable<Object, Object> c) 
            throws IOException {
        final int size = _in.readInt();
        c.clear();
        Object key = null;
        Object val = null;
        List<MapEntry> values = new ArrayList<MapEntry>();
        for (int i=0; i < size; i++) {
            //c.put(deserializeObject(), deserializeObject());
            //The following check is necessary where the content of the 
            //Collection contains restricted objects, in which case 'null'
            //is transferred.
            key = deserializeObject();
            val = deserializeObject();
            if (key != null && val != null) {
                //We don't fill the Map here, see SPR 5493
                //c.put(key, val);
                values.add(new MapEntry(key, val));
            }                
        }
        _mapsToFill.add(new MapValuePair(c, values));
    }
    
    private final void deserializeDBLargeVector(DBLargeVector c) throws IOException {
        final int size = _in.readInt();
        c.clear();
        Object val = null;
        for (int i=0; i < size; i++) {
            val = deserializeObject();
            if (val != null) {
                c.add(val);
            }
        }
    }

    private final void deserializeDBVector(DBVector<Object> c) throws IOException {
        final int size = _in.readInt();
        c.clear();
        Object val = null;
        for (int i=0; i < size; i++) {
            val = deserializeObject();
            if (val != null) {
                c.add(val);
            }
        }
    }
    
    private final String deserializeString() {
    	return _in.readString();
    }

    private final Class<?> readClassInfo() throws IOException {
        final short id = _in.readShort();
        if (id < -1 || id > _usedClasses.size()) {
            throw new PropagationCorruptedException(
                    "ID (max=" + _usedClasses.size() + "): " + id);
        }
        if (id == -1) {
            //null-reference
            return null;
        }
        if (id > 0) {
            return _usedClasses.get(id - 1);
        }
        
        //if id==0 read the class
        String cName = deserializeString();
        try {
        	//TODO remove this once we have a list of standard classes!
            Class<?> cls = null;
            if (cName.equals("boolean")) {
                cls = Boolean.TYPE;
            } else if (cName.equals("byte")) {
                cls = Byte.TYPE;
            } else if (cName.equals("char")) {
                cls = Character.TYPE;
            } else if (cName.equals("double")) {
                cls = Double.TYPE;
            } else if (cName.equals("float")) {
                cls = Float.TYPE;
            } else if (cName.equals("int")) {
                cls = Integer.TYPE;
            } else if (cName.equals("long")) {
                cls = Long.TYPE;
            } else if (cName.equals("short")) {
                cls = Short.TYPE;
            } else {
                cls = Class.forName(cName);
            }
            _usedClasses.add(cls);
            return cls;
        } catch (ClassNotFoundException e) {
            throw new PropagationCorruptedException(
                    "Class not found: \"" + cName + "\" (" + id + ")", e);
        }
    }
    
    private static final Object createInstance(Class<?> cls) {
        try {
        	//TODO remove special treatment. Allow Serializable / Externalizable? Via Properties?
            if (File.class.isAssignableFrom(cls)) {
                return new File("");
            }
            //find the constructor
            Constructor<?> c = _defaultConstructors.get(cls);
            if (c == null) {
                c = cls.getDeclaredConstructor((Class[])null);
                c.setAccessible(true);
                _defaultConstructors.put(cls, c);
            }
            //use the constructor
            return c.newInstance();
        } catch (SecurityException e1) {
            throw new RuntimeException(e1);
        } catch (NoSuchMethodException e1) {
            throw new RuntimeException(e1);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final PersistenceCapableImpl createSizedInstance(Class<?> cls, int size) {
        if (size <= 0) {
            return (PersistenceCapableImpl) createInstance(cls);
        }
        try {
            //find the constructor
            Constructor<?> c = _sizedConstructors.get(cls);
            if (c == null) {
                c = cls.getDeclaredConstructor(new Class[]{Integer.TYPE});
                c.setAccessible(true);
                _sizedConstructors.put(cls, c);
            }
            //use the constructor
            return (PersistenceCapableImpl) c.newInstance(new Object[]{size});
        } catch (SecurityException e1) {
            throw new RuntimeException(e1);
        } catch (NoSuchMethodException e1) {
            throw new RuntimeException(e1);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

   //TODO rename to setOid/setPersistentState
    //TODO merge with createdumy & createObject
    final void prepareObject(PersistenceCapableImpl obj, long oid, boolean hollow) {
        obj.jdoZooSetOid(oid);
        obj.jdoReplaceStateManager(_cache.getStateManager());
//        obj.jdoNewInstance(sm); //?
        
//        System.out.println("HOLLOW: " + obj.jdoZooGetOid() + " " + obj.jdoGetObjectId());
        if (hollow) {
        	_cache.addHollow(obj, _node);
        } else {
        	_cache.addPC(obj, _node);
        }
    }
    
    private static final boolean isPersistentCapableClass(Class<?> cls) {
        return PersistenceCapableImpl.class.isAssignableFrom(cls);
    }
    
    private final Object hollowForOid(long oid, Class<?> cls) {
        if (oid == 0) {
            throw new IllegalArgumentException();
        }
        
        //check cache
    	CachedObject co = _cache.findCoByOID(oid);
        if (co != null) {
        	//Object exist.
            return co.getObject();
        }
        
        PersistenceCapableImpl obj = (PersistenceCapableImpl) createInstance(cls);
        prepareObject(obj, oid, true);
        return obj;
    }
}
