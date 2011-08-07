package org.zoodb.jdo.internal;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBLargeVector;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.internal.SerializerTools.PRIMITIVE;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.client.CachedObject;
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

    private final SerialInput _in;
    
    //Here is how class information is transmitted:
    //If the class does not exist in the hashMap, then it is added and its 
    //name is written to the stream. Otherwise only the id of the class in the 
    //List in written.
    //The class information is required because it can be any sub-type of the
    //Field type, but the exact type is required for instantiation.
    private final ArrayList<Class<?>> _usedClasses = new ArrayList<Class<?>>(20);

    //Using static concurrent Maps seems to be 30% faster than non-static local maps that are 
    //created again and again.
    private static final ConcurrentHashMap<Class<?>, Constructor<?>> _defaultConstructors = 
        new ConcurrentHashMap<Class<?>, Constructor<?>>(100);
    
    private final AbstractCache _cache;
    private final Node _node;
    
    //Cached Sets and Maps
    //The maps and sets are only filled after the keys have been de-serialized. Otherwise 
    //the keys will be inserted with a wrong hash value.
    //TODO load MAPS and SETS in one go and load all keys right away!
    //TODO or do not use add functionality, but serialize internal arrays right away! Probably does
    //not work for mixtures of LinkedLists and set like black-whit tree. (?).
    private final List<MapValuePair> _mapsToFill = new LinkedList<MapValuePair>();
    private final List<SetValuePair> _setsToFill = new LinkedList<SetValuePair>();
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
    public DataDeSerializer(SerialInput in, AbstractCache cache, Node node) {
        _in = in;
        _cache = cache;
        _node = node;
    }


	/**
     * This method returns an object that is read from the input 
     * stream.
     * @param pos 
     * @return The read object.
     */
	public PersistenceCapableImpl readObject(long pos) {
		_in.seekPos(pos, true);
		return readObject();
	}

	/**
     * This method returns an object that is read from the input 
     * stream.
     * @param page 
     * @param offs 
     * @return The read object.
     */
    public PersistenceCapableImpl readObject(int page, int offs) {
		_in.seekPage(page, offs, true);
		return readObject();
    }
    
    private PersistenceCapableImpl readObject() {
    	//Read object header. This allows pre-initialisation of object,
        //which is helpful in case a later object is referenced by an 
        //earlier one.
        //Read first object:
    	long oid = _in.readLong();
        //read class info:
    	long clsOid = _in.readLong();
    	ZooClassDef clsDef = _cache.getSchema(clsOid);
        PersistenceCapableImpl pObj = readPersistentObjectHeader(clsDef, oid);
        deserializeFields1( pObj, pObj.getClass(), clsDef );
        deserializeFields2( pObj, pObj.getClass(), clsDef );

        
        List<PersistenceCapableImpl> preLoaded = null;
        List<ZooClassDef> preLoadedDefs = null;
        if (pObj instanceof Map || pObj instanceof Set) {
            preLoaded = new LinkedList<PersistenceCapableImpl>();
            preLoadedDefs = new LinkedList<ZooClassDef>();
        	//TODO this is also important for sorted collections!
            int nH = _in.readInt();
            for (int i = 0; i < nH; i++) {
                //read class info:
            	long clsOid2 = _in.readLong();
            	ZooClassDef clsDef2 = _cache.getSchema(clsOid2);
            	long oid2 = _in.readLong();
                PersistenceCapableImpl obj = readPersistentObjectHeader(clsDef2, oid2);
                preLoaded.add(obj);
                preLoadedDefs.add(clsDef2);
            }
        }
        
        deserializeSpecial( pObj, pObj.getClass() );

        //read objects data
        if (preLoadedDefs != null) {
	        Iterator<ZooClassDef> iter = preLoadedDefs.iterator();
	        for (PersistenceCapableImpl obj: preLoaded) {
	            try {
	            	ZooClassDef def = iter.next();
	                deserializeFields1( obj, obj.getClass(), def );
	                deserializeFields2( obj, obj.getClass(), def );
	                deserializeSpecial( obj, obj.getClass() );
	            } catch (DataStreamCorruptedException e) {
	                DatabaseLogger.severe("Corrupted Object ID: " + obj.getClass());
	                throw e;
	            }
	        }
        }

        //Rehash collections. We have to do add all keys again, 
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
        
        return pObj;
    }
    
    private final PersistenceCapableImpl readPersistentObjectHeader(ZooClassDef clsDef, long oid) {
		Class<?> cls = clsDef.getJavaClass(); 
            
    	PersistenceCapableImpl obj = null;
    	CachedObject co = _cache.findCoByOID(oid);
    	if (co != null) {
    		//might be hollow!
    		co.markClean();
    		obj = co.obj;
            return obj;
        }
        
    	obj = (PersistenceCapableImpl) createInstance(cls);
    	prepareObject(obj, oid, false);
        return obj;
    }

    private final Object deserializeFields1(Object obj, Class<?> cls, ZooClassDef clsDef) {
        Field f1 = null;
        Object deObj = null;
        try {
            //Read fields
        	for (ZooFieldDef fd: clsDef.getAllFields()) {
                Field f = fd.getJavaField();
                f1 = f;
                PRIMITIVE prim = fd.getPrimitiveType();
                if (prim != null) {
                	deserializePrimitive(obj, f, prim);
                } else if (fd.isFixedSize()) {
                	deObj = deserializeObjectNoSco(fd);
                    f.set(obj, deObj);
                }
        	}
            return obj;
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (DataStreamCorruptedException e) {
            throw new DataStreamCorruptedException("Corrupted Object: " +
                    //Util.getOidAsString(obj) + 
                    " " + cls + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 , e);
        }
    }

    private final Object deserializeFields2(Object obj, Class<?> cls, ZooClassDef clsDef) {
        Field f1 = null;
        Object deObj = null;
        try {
            //Read fields
        	for (ZooFieldDef fd: clsDef.getAllFields()) {
                if (!fd.isFixedSize() || fd.isString()) {
                	Field f = fd.getJavaField();
                	f1 = f;
                   	deObj = deserializeObjectSCO();
                    f.set(obj, deObj);
                }
        	}
            return obj;
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Field: " + f1.getType() + " " + f1.getName(), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (DataStreamCorruptedException e) {
            throw new DataStreamCorruptedException("Corrupted Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 , e);
        }
    }

    private final Object deserializeSCO(Object obj, Class<?> cls) {
        Field f1 = null;
        Object deObj = null;
        try {
            //Read fields
            for (Field field: SerializerTools.getFields(cls)) {
                f1 = field;
                if (field.getType().isPrimitive()) {
                    PRIMITIVE prim = SerializerTools.PRIMITIVE_TYPES.get(field.getType());
                	deserializePrimitive(obj, field, prim);
                } else {
                	deObj = deserializeObject();
                    field.set(obj, deObj);
                }
            }
            return obj;
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (DataStreamCorruptedException e) {
            throw new DataStreamCorruptedException("Corrupted Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 , e);
        }
    }

    @SuppressWarnings("unchecked")
    private final Object deserializeSpecial(Object obj, Class<?> cls) {
        Field f1 = null;
        try {
            //Special treatment for persistent containers.
            //Their data is not stored in (visible) fields.
            if (obj instanceof DBHashtable) {
                deserializeDBHashtable((DBHashtable<Object, Object>) obj);
            } else if (obj instanceof DBLargeVector) {
                deserializeDBLargeVector((DBLargeVector<Object>) obj);
            } else if (obj instanceof DBVector) {
                deserializeDBVector((DBVector<Object>) obj);
            }
            return obj;
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 , e);
        }
    }

    private final void deserializePrimitive(Object parent, Field field, PRIMITIVE prim) 
            throws IllegalArgumentException, IllegalAccessException {
        switch (prim) {
        case BOOLEAN: field.setBoolean(parent, _in.readBoolean()); break;
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
    }        
             
    private final Object deserializeObjectNoSco(ZooFieldDef def) {
        //read class/null info
        Class<?> cls = readClassInfo();
        if (cls == null) {
        	_in.skipRead(def.getLength()-1);
            //reference is null
            return null;
        }

        //read instance data
        if (def.isPersistentType()) {
            long oid = _in.readLong();

            //Is object already in the database or cache?
            Object obj = hollowForOid(oid, cls);
            return obj;
        } else if (String.class == cls) {
        	_in.readLong(); //read and ignore magic number
            return null;
        } else if (Date.class == cls) {
            return new Date(_in.readLong());
        }

        throw new IllegalStateException("Illegal type: " + def.getName());
    }

    @SuppressWarnings("unchecked")
    private final Object deserializeObjectSCO() {
        //read class/null info
        Class<?> cls = readClassInfo();
        if (cls == null) {
            //reference is null
            return null;
        }
        
        if (cls.isArray()) {
            return deserializeArray();
        }
        
        PRIMITIVE p;
        
        //read instance data
        if (isPersistentCapableClass(cls)) {
        	//this can happen when we have a persistent object in a field of a non-persistent type
        	//like Object or possibly an interface
            long oid = _in.readLong();

            //Is object already in the database or cache?
            Object obj = hollowForOid(oid, cls);
            return obj;
//        } else if (SerializerTools.PRIMITIVE_CLASSES.containsKey(cls)) {
        } else if ((p = SerializerTools.PRIMITIVE_CLASSES.get(cls)) != null) {
            return deserializeNumber(p);
        } else if (String.class == cls) {
            return deserializeString();
        } else if (Date.class == cls) {
        	throw new IllegalStateException();
        }
        
        if (Map.class.isAssignableFrom(cls)) {
            //ordered 
            int len = _in.readInt();
            Map<Object, Object> m = (Map<Object, Object>) createInstance(cls);  //TODO sized?
            List<MapEntry> values = new ArrayList<MapEntry>(len);
            for (int i=0; i < len; i++) {
                //m.put(deserializeObject(), deserializeObject());
                //We don't fill the Map here.
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
                //We don't fill the Set here.
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
        Object oo = deserializeSCO(createInstance(cls), cls);
        deserializeSpecial(oo, cls);
        return oo;
    }

    /**
     * De-serialize objects. If the object is persistent capable, only it's OID
     * is stored. Otherwise it is serialized and the method is called
     * recursively on all of it's fields.
     * @return De-serialized value.
     */
    @SuppressWarnings("unchecked")
    private final Object deserializeObject() {
        //read class/null info
        Class<?> cls = readClassInfo();
        if (cls == null) {
            //reference is null
            return null;
        }
        
        if (cls.isArray()) {
            return deserializeArray();
        }
        
        PRIMITIVE p;
        
        //read instance data
        if (isPersistentCapableClass(cls)) {
            long oid = _in.readLong();

            //Is object already in the database or cache?
            Object obj = hollowForOid(oid, cls);
            return obj;
        } else if ((p = SerializerTools.PRIMITIVE_CLASSES.get(cls)) != null) {
            return deserializeNumber(p);
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
                //We don't fill the Map here.
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
                //We don't fill the Set here.
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
        Object oo = deserializeSCO(createInstance(cls), cls);
        deserializeSpecial(oo, cls);
        return oo;
    }

    private final Object deserializeNumber(PRIMITIVE prim) {
        switch (prim) {
        case BOOLEAN: return _in.readBoolean();
        case BYTE: return _in.readByte();
        case CHAR: return _in.readChar();
        case DOUBLE: return _in.readDouble();
        case FLOAT: return _in.readFloat();
        case INT: return _in.readInt();
        case LONG: return _in.readLong();
        case SHORT: return _in.readShort();
        default: throw new UnsupportedOperationException(
                "Class not supported: " + prim);
        }
    }
    
    private final Object deserializeArray() {
        
        // read meta data
        Class<?> innerType = readClassInfo();
        String innerTypeAcronym = deserializeString();
        
        short dims = _in.readShort();
        
        // read data
        return deserializeArrayColumn(innerType, innerTypeAcronym, dims);
    }

    private final Object deserializeArrayColumn(Class<?> innerType, 
            String innerAcronym, int dims) {

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
                Class<?> compClass =  Class.forName(new String(ca) + innerAcronym);
                array = Array.newInstance(compClass, l);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < l; i++) {
                Array.set(array, i,  deserializeArrayColumn(innerType, innerAcronym, dims-1) );
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

    private final void deserializeDBHashtable(DBHashtable<Object, Object> c) {
        final int size = _in.readInt();
        c.clear();
        c.resize(size);
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
                //We don't fill the Map here.
                //c.put(key, val);
                values.add(new MapEntry(key, val));
            }                
        }
        _mapsToFill.add(new MapValuePair(c, values));
    }
    
    private final void deserializeDBLargeVector(DBLargeVector<Object> c) {
        final int size = _in.readInt();
        c.clear();
        c.resize(size);
        Object val = null;
        for (int i=0; i < size; i++) {
            val = deserializeObject();
            if (val != null) {
                c.add(val);
            }
        }
    }

    private final void deserializeDBVector(DBVector<Object> c) {
        final int size = _in.readInt();
        c.clear();
        c.resize(size);
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

    private final Class<?> readClassInfo() {
    	final byte id = _in.readByte();
    	switch (id) {
    	//null-reference
    	case -1: return null;
    	case SerializerTools.REF_PERS_ID: {
    		long soid = _in.readLong();
    		//Schema Evolution
    		//================
    		//Maybe we need to create an OID->Schema-OID index?
    		//Due to schema evolution, the Schema-OID in serialized references may be out-dated with
    		//respect to the referenced object. Generally, it may be impossible to create a hollow 
    		//object from the OID.
    		//Alternative: look up the schema and create a hollow of the latest version?!?!?     
    		//TODO ZooClassDef def = _cache.getSchemaLatestVersion(soid);
    		ZooClassDef def = _cache.getSchema(soid);
    		if (def.getJavaClass() != null) {
    			return def.getJavaClass();
    		}
    		try {
    			return Class.forName(def.getClassName());
    		} catch (ClassNotFoundException e) {
    			throw new DataStreamCorruptedException("Class not found: " + def.getClassName(), e);
    		}
    	}
    	case SerializerTools.REF_ARRAY_ID: {
    		//an array
    		return byte[].class;
    	}
    	case 0: {
    		//if id==0 read the class
    		String cName = deserializeString();
    		try {
    			Class<?> cls = Class.forName(cName);
    			_usedClasses.add(cls);
    			return cls;
    		} catch (ClassNotFoundException e) {
    			throw new DataStreamCorruptedException(
    					"Class not found: \"" + cName + "\" (" + id + ")", e);
    		}
    	}
    	}
    	if (id > 0) {
    		if (id < SerializerTools.REF_CLS_OFS) {
    			return SerializerTools.PRE_DEF_CLASSES_ARRAY.get(id);
    		} else {
    			return _usedClasses.get(id - 1 - SerializerTools.REF_CLS_OFS);
    		}	
    	}

//        if (id == -1) {
//            //null-reference
//            return null;
//        }
//        if (id == SerializerTools.REF_PERS_ID) {
//        	long soid = _in.readLong();
//        	//Schema Evolution
//        	//================
//        	//Maybe we need to create an OID->Schema-OID index?
//        	//Due to schema evolution, the Schema-OID in serialized references may be out-dated with
//        	//respect to the referenced object. Generally, it may be impossible to create a hollow 
//        	//object from the OID.
//        	//Alternative: look up the schema and create a hollow of the latest version?!?!?     
//        	//TODO ZooClassDef def = _cache.getSchemaLatestVersion(soid);
//        	ZooClassDef def = _cache.getSchema(soid);
//        	if (def.getJavaClass() != null) {
//        		return def.getJavaClass();
//        	}
//        	try {
//				return Class.forName(def.getClassName());
//			} catch (ClassNotFoundException e) {
//				throw new DataStreamCorruptedException("Class not found: " + def.getClassName(), e);
//			}
//        }
//        if (id > 0 && id < SerializerTools.REF_CLS_OFS) {
//            return SerializerTools.PRE_DEF_CLASSES_ARRAY.get(id);
//        }
//        if (id > 0) {
//            return _usedClasses.get(id - 1 - SerializerTools.REF_CLS_OFS);
//        }
//        
//        if (id == 0) {
//            //if id==0 read the class
//            String cName = deserializeString();
//            try {
//                Class<?> cls = Class.forName(cName);
//                _usedClasses.add(cls);
//                return cls;
//            } catch (ClassNotFoundException e) {
//                throw new DataStreamCorruptedException(
//                        "Class not found: \"" + cName + "\" (" + id + ")", e);
//            }
//        }
        throw new DataStreamCorruptedException("ID (max=" + _usedClasses.size() + "): " + id);
    }
    
    private final Object createInstance(Class<?> cls) {
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
    
   //TODO rename to setOid/setPersistentState
    //TODO merge with createdumy & createObject
    final void prepareObject(PersistenceCapableImpl obj, long oid, boolean hollow) {
        obj.jdoZooSetOid(oid);
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
