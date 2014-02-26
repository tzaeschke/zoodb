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
package org.zoodb.tools.internal;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jdo.ObjectState;

import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBHashMap;
import org.zoodb.api.DBLargeVector;
import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.BinaryDataCorruptedException;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.SerializerTools.PRIMITIVE;
import org.zoodb.internal.util.Util;
import org.zoodb.tools.internal.ObjectCache.GOProxy;


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

    private XmlReader in;
    
    //Here is how class information is transmitted:
    //If the class does not exist in the hashMap, then it is added and its 
    //name is written to the stream. Otherwise only the id of the class in the 
    //List in written.
    //The class information is required because it can be any sub-type of the
    //Field type, but the exact type is required for instantiation.
    private final ArrayList<Class<?>> usedClasses = new ArrayList<Class<?>>(20);

    //Using static concurrent Maps seems to be 30% faster than non-static local maps that are 
    //created again and again.
    private static final ConcurrentHashMap<Class<?>, Constructor<?>> DEFAULT_CONSTRUCTORS = 
        new ConcurrentHashMap<Class<?>, Constructor<?>>(100);
    
    private final ObjectCache cache;
    
    //Cached Sets and Maps
    //The maps and sets are only filled after the keys have been de-serialized. Otherwise 
    //the keys will be inserted with a wrong hash value.
    //TODO load MAPS and SETS in one go and load all keys right away!
    //TODO or do not use add functionality, but serialize internal arrays right away! Probably does
    //not work for mixtures of LinkedLists and set like black-whit tree. (?).
    private final ArrayList<MapValuePair> mapsToFill = new ArrayList<MapValuePair>(5);
    private final ArrayList<SetValuePair> setsToFill = new ArrayList<SetValuePair>(5);
    private static class MapEntry { 
        Object K; Object V; 
        public MapEntry(Object key, Object value) {
            K = key;
            V = value;
        }
    }
    private static class MapValuePair { 
        Map<Object, Object> map; 
        MapEntry[] values; 
        public MapValuePair(Map<Object, Object> map, MapEntry[] values) {
            this.map = map;
            this.values = values;
        }
    }
    private static class SetValuePair { 
        Set<Object> set; 
        Object[] values; 
        public SetValuePair(Set<Object> set, Object[] values) {
        	this.set = set;
        	this.values = values;
        }
    }
    
    /**
     * Create a new DataDeserializer.
     * @param in Stream to read the data from.
     * persistent.
     */
    public DataDeSerializer(XmlReader in, ObjectCache cache) {
        this.in = in;
        this.cache = cache;
    }


    public void readGenericObject(long oid, long clsOid, GOProxy hdl) {
        ZooClassDef clsDef = cache.getSchema(clsOid);

    	// read object
        deserializeFieldsGO( hdl.getGenericObject(), clsDef );
        
        deserializeSpecialGO(hdl.getGenericObject());
        
        postProcessCollections();

        usedClasses.clear();
    }
    
    private final Object deserializeFieldsGO(GenericObject obj, ZooClassDef clsDef) {
        ZooFieldDef f1 = null;
        Object deObj = null;
        try {
            //Read fixed size fields
        	int i = 0;
        	for (ZooFieldDef fd: clsDef.getAllFields()) {
        		in.startReadingField(fd.getFieldPos());
                f1 = fd;
                PRIMITIVE prim = fd.getPrimitiveType();
                if (prim != null) {
                	deObj = deserializePrimitive(prim);
                    obj.setFieldRAW(i, deObj);
                } else if (fd.isFixedSize()) {
                	deObj = deserializeObjectNoSco(fd);
               		obj.setField(fd, deObj);
                } else {
                   	deObj = deserializeObjectSCO();
                    obj.setFieldRawSCO(i, deObj);
                }
        		in.stopReadingField();
                i++;
        	}
            return obj;
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (SecurityException e) {
            throw new RuntimeException(e);
        } catch (BinaryDataCorruptedException e) {
            throw new BinaryDataCorruptedException("Corrupted Object: " +
                    //Util.getOidAsString(obj) + 
                    " " + clsDef + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + clsDef + " F:" + f1 , e);
        }
    }

    private void postProcessCollections() {
        //Rehash collections. We have to do add all keys again, 
        //because when the collections were first de-serialised, the keys may
        //not have been de-serialised yet (if persistent) therefore their
        //hash-code may have been wrong.
        for (SetValuePair sv: setsToFill) {
            sv.set.clear();
            for (Object o: sv.values) {
                sv.set.add(o);
            }
        }
        setsToFill.clear();
        for (MapValuePair mv: mapsToFill) {
            //TODO NPE may occur because of skipping elements in deserializeHashTable (line 711)
            mv.map.clear();
            for (MapEntry e: mv.values) {
                mv.map.put(e.K, e.V);
            }
        }
        mapsToFill.clear();
        usedClasses.clear();
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
//        } catch (DataFormatException e) {
//            throw new DataFormatException("Corrupted Object: " +
//                    Util.getOidAsString(obj) + " " + "" + " F:" + 
//                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 , e);
        }
    }

    private final void deserializeSpecialGO(GenericObject obj) {
    	if (obj.getClassDef().getClassName().equals(DBHashMap.class.getName())) {
    		in.startReadingField(-1);
            //Special treatment for persistent containers.
            //Their data is not stored in (visible) fields.
    		HashMap<Object, Object> m = new HashMap<Object, Object>();
    		obj.setDbCollection(m);
    		deserializeDBHashMap(m);
    		in.stopReadingField();
    	} else if (obj.getClassDef().getClassName().equals(DBLargeVector.class.getName())) {
    		in.startReadingField(-1);
    		ArrayList<Object> l = new ArrayList<Object>();
    		obj.setDbCollection(l);
    		deserializeDBList(l);
    		in.stopReadingField();
    	} else if (obj.getClassDef().getClassName().equals(DBArrayList.class.getName())) {
    		in.startReadingField(-1);
    		ArrayList<Object> l = new ArrayList<Object>();
    		obj.setDbCollection(l);
    		deserializeDBList(l);
    		in.stopReadingField();
    	}
    }

    private final void deserializePrimitive(Object parent, Field field, PRIMITIVE prim) 
            throws IllegalArgumentException, IllegalAccessException {
        switch (prim) {
        case BOOLEAN: field.setBoolean(parent, in.readBoolean()); break;
        case BYTE: field.setByte(parent, in.readByte()); break;
        case CHAR: field.setChar(parent, in.readChar()); break;
        case DOUBLE: field.setDouble(parent, in.readDouble()); break;
        case FLOAT: field.setFloat(parent, in.readFloat()); break;
        case INT: field.setInt(parent, in.readInt()); break;
        case LONG: field.setLong(parent, in.readLong()); break;
        case SHORT: field.setShort(parent, in.readShort()); break;
        default:
            throw new UnsupportedOperationException(prim.toString());
        }
    }        
             
    private final Object deserializePrimitive(PRIMITIVE prim) 
    throws IllegalArgumentException, IllegalAccessException {
    	switch (prim) {
    	case BOOLEAN: return in.readBoolean();
    	case BYTE: return in.readByte();
    	case CHAR: return in.readChar();
    	case DOUBLE: return in.readDouble();
    	case FLOAT: return in.readFloat();
    	case INT: return in.readInt();
    	case LONG: return in.readLong();
    	case SHORT: return in.readShort();
    	default:
    		throw new UnsupportedOperationException(prim.toString());
    	}
    }        

    private final Object deserializeObjectNoSco(ZooFieldDef def) {
        //read class/null info
        Object cls = readClassInfo();
        if (cls == null) {
            //reference is null
            return null;
        }

        //read instance data
        if (ZooClassDef.class.isAssignableFrom(cls.getClass())) {
            long oid = in.readLong();
            //for GOs we just store the OID
            return getGO(oid, (ZooClassDef)cls);
        } else if (String.class == cls) {
            return in.readString();
        } else if (Date.class == cls) {
            return new Date(in.readLong());
        }

        throw new IllegalArgumentException("Illegal type: " + def.getName() + ": " + 
                def.getTypeName() + " in class " + def.getDeclaringType().getClassName());
    }

    @SuppressWarnings("unchecked")
    private final Object deserializeObjectSCO() {
        //read class/null info
        Object clsO = readClassInfo();
        if (clsO == null) {
            //reference is null
            return null;
        }
        
        if (ZooClassDef.class.isAssignableFrom(clsO.getClass())) {
        	//this can happen when we have a persistent object in a field of a non-persistent type
        	//like Object or possibly an interface
            long oid = in.readLong();

            //Is object already in the database or cache?
            Object obj = getGO(oid, (ZooClassDef)clsO);
            return obj;
        }

        Class<?> cls = (Class<?>) clsO;
        if (cls.isArray()) {
            return deserializeArray();
        }
        if (cls.isEnum()) {
        	return deserializeEnum();
        }
        
        PRIMITIVE p;
        
        //read instance data
        if ((p = SerializerTools.PRIMITIVE_CLASSES.get(cls)) != null) {
            return deserializeNumber(p);
        } else if (String.class == cls) {
            return deserializeString();
        } else if (Date.class == cls) {
        	throw new IllegalStateException();
        }
        
        if (Map.class.isAssignableFrom(cls)) {
            //ordered 
            int len = in.readInt();
            Map<Object, Object> m = (Map<Object, Object>) createInstance(cls);  //TODO sized?
            MapEntry[] values = new MapEntry[len];
            for (int i=0; i < len; i++) {
                //m.put(deserializeObject(), deserializeObject());
                //We don't fill the Map here.
                values[i] = new MapEntry(deserializeObject(), deserializeObject());
            }
            mapsToFill.add(new MapValuePair(m, values));
            return m;
        }
        if (Set.class.isAssignableFrom(cls)) {
            //ordered 
            int len = in.readInt();
            Set<Object> s = (Set<Object>) createInstance(cls);  //TODO sized?
            Object[] values = new Object[len];
            for (int i=0; i < len; i++) {
                //s.add(deserializeObject());
                //We don't fill the Set here.
                values[i] = deserializeObject();
            }
            setsToFill.add(new SetValuePair(s, values));
            return s;
        }
        //Check Iterable, Map, 'Array'  
        //This would include Vector and Hashtable
        if (Collection.class.isAssignableFrom(cls)) {
            Collection<Object> l = (Collection<Object>) createInstance(cls);  //TODO sized?
            //ordered 
            int len = in.readInt();
            for (int i=0; i < len; i++) {
                l.add(deserializeObject());
            }
            return l;
        }
        
        // TODO disallow? Allow Serializable/ Externalizable
        Object oo = deserializeSCO(createInstance(cls), cls);
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
        Object clsO = readClassInfo();
        if (clsO == null) {
            //reference is null
            return null;
        }
        
        if (ZooClassDef.class.isAssignableFrom(clsO.getClass())) {
        	long oid = in.readLong();

        	//Is object already in the database or cache?
        	Object obj = getGO(oid, (ZooClassDef)clsO);
        	return obj;
        }
        
        Class<?> cls = (Class<?>) clsO;
        if (cls.isArray()) {
            return deserializeArray();
        }
        if (cls.isEnum()) {
        	return deserializeEnum();
        }
       
        PRIMITIVE p;
        
        //read instance data
        if ((p = SerializerTools.PRIMITIVE_CLASSES.get(cls)) != null) {
            return deserializeNumber(p);
        } else if (String.class == cls) {
            return deserializeString();
        } else if (Date.class == cls) {
            return new Date(in.readLong());
        }
        
        if (Map.class.isAssignableFrom(cls)) {
            //ordered 
            int len = in.readInt();
            Map<Object, Object> m = (Map<Object, Object>) createInstance(cls);  //TODO sized?
            MapEntry[] values = new MapEntry[len];
            for (int i=0; i < len; i++) {
                //m.put(deserializeObject(), deserializeObject());
                //We don't fill the Map here.
                values[i] = new MapEntry(deserializeObject(), deserializeObject());
            }
            mapsToFill.add(new MapValuePair(m, values));
            return m;
        }
        if (Set.class.isAssignableFrom(cls)) {
            //ordered 
            int len = in.readInt();
            Set<Object> s = (Set<Object>) createInstance(cls);  //TODO sized?
            Object[] values = new Object[len];
            for (int i=0; i < len; i++) {
                //s.add(deserializeObject());
                //We don't fill the Set here.
                values[i] = deserializeObject();
            }
            setsToFill.add(new SetValuePair(s, values));
            return s;
        }
        //Check Iterable, Map, 'Array'  
        //This would include Vector and Hashtable
        if (Collection.class.isAssignableFrom(cls)) {
            Collection<Object> l = (Collection<Object>) createInstance(cls);  //TODO sized?
            //ordered 
            int len = in.readInt();
            for (int i=0; i < len; i++) {
                l.add(deserializeObject());
            }
            return l;
        }
        
        // TODO disallow? Allow Serializable/ Externalizable
        System.out.println("Deserializing SCO: " + cls.getName());
        Object oo = deserializeSCO(createInstance(cls), cls);
        return oo;
    }

    private final Object deserializeNumber(PRIMITIVE prim) {
        switch (prim) {
        case BOOLEAN: return in.readBoolean();
        case BYTE: return in.readByte();
        case CHAR: return in.readChar();
        case DOUBLE: return in.readDouble();
        case FLOAT: return in.readFloat();
        case INT: return in.readInt();
        case LONG: return in.readLong();
        case SHORT: return in.readShort();
        default: throw new UnsupportedOperationException(
                "Class not supported: " + prim);
        }
    }
    
    private final Object deserializeEnum() {
        // read meta data
        Class<?> enumType = (Class<?>) readClassInfo();
        short value = in.readShort();
		return enumType.getEnumConstants()[value];
    }

   private final Object deserializeArray() {
        
        // read meta data
	   	Object innerType = readClassInfo();
	   	if (ZooClassDef.class.isAssignableFrom(innerType.getClass())) {
	   		innerType = cache.getGopClass(((ZooClassDef)innerType).getOid());
	   	}
        String innerTypeAcronym = deserializeString();
        
        short dims = in.readShort();
        
        // read data
        return deserializeArrayColumn((Class<?>) innerType, innerTypeAcronym, dims);
    }

    private final Object deserializeArrayColumn(Class<?> innerType, String innerAcronym, int dims) {
        //read length
        int l = in.readInt();
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
                    a[i] = in.readBoolean();
                }
            } else if (innerType == Byte.TYPE) {
                in.readFully((byte[])array);
            } else if (innerType == Character.TYPE) {
                char[] a = (char[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = in.readChar();
                }
            } else if (innerType == Float.TYPE) {
                float[] a = (float[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = in.readFloat();
                }
            } else if (innerType == Double.TYPE) {
                double[] a = (double[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = in.readDouble();
                }
            } else if (innerType == Integer.TYPE) {
                int[] a = (int[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = in.readInt();
                }
            } else if (innerType == Long.TYPE) {
                long[] a = (long[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = in.readLong();
                }
            } else if (innerType == Short.TYPE) {
                short[] a = (short[])array;
                for (int i = 0; i < l; i++) {
                    a[i] = in.readShort();
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

    private final void deserializeDBHashMap(HashMap<Object, Object> c) {
        final int size = in.readInt();
        c.clear();
        //c.resize(size);
        Object key = null;
        Object val = null;
        MapEntry[] values = new MapEntry[size];
        for (int i=0; i < size; i++) {
            //c.put(deserializeObject(), deserializeObject());
            //The following check is necessary where the content of the 
            //Collection contains restricted objects, in which case 'null'
            //is transferred.
            key = deserializeObject();
            val = deserializeObject();
            //We don't fill the Map here, because hashCodes rely on fully loaded objects.
            //c.put(key, val);
            values[i] = new MapEntry(key, val);
        }
        mapsToFill.add(new MapValuePair(c, values));
    }
    
    private final void deserializeDBList(ArrayList<Object> c) {
        final int size = in.readInt();
        c.clear();
        //c.resize(size);
        Object val = null;
        for (int i=0; i < size; i++) {
            val = deserializeObject();
            if (val != null) {
                c.add(val);
            }
        }
    }
    
    private final String deserializeString() {
    	return in.readString();
    }

    private final Object readClassInfo() {
    	final byte id = in.readByte();
    	switch (id) {
    	//null-reference
    	case -1: return null;
    	case SerializerTools.REF_PERS_ID: {
    		long soid = in.readLong();
    		ZooClassDef cls = cache.getClass(soid);
    		if (cls != null) {
    			return cls;
    		}
    		throw new IllegalStateException("Class not found: " + soid);
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
    			usedClasses.add(cls);
    			return cls;
    		} catch (ClassNotFoundException e) {
    			if (cName.length() > 100) {
    				cName = cName.substring(0, 100);
    			}
    			//Do not embed 'e' to avoid problems with excessively long class names.
    			throw new BinaryDataCorruptedException(
    					"Class not found: \"" + cName + "\" (" + id + ")", e);
    		}
    	}
    	default: {
    		if (id < SerializerTools.REF_CLS_OFS) {
    			return SerializerTools.PRE_DEF_CLASSES_ARRAY.get(id);
    		} else {
    			return usedClasses.get(id - 1 - SerializerTools.REF_CLS_OFS);
    		}	
    	}
    	}
    }
    
    private final Object createInstance(Class<?> cls) {
        try {
            //find the constructor
            Constructor<?> c = DEFAULT_CONSTRUCTORS.get(cls);
            if (c == null) {
                //TODO remove special treatment. Allow Serializable / Externalizable? Via Properties?
                if (File.class.isAssignableFrom(cls)) {
                    return new File("");
//                } else if (ZooFieldDef.class.isAssignableFrom(cls)) {
//                	return new ZooFieldDef(in);
                }
                c = cls.getDeclaredConstructor((Class[])null);
                c.setAccessible(true);
                DEFAULT_CONSTRUCTORS.put(cls, c);
            }
            //use the constructor
            return c.newInstance();
        } catch (SecurityException e1) {
            throw new RuntimeException(e1);
        } catch (NoSuchMethodException e1) {
            throw new RuntimeException("Class requires default constructor (can be private): " + 
            		cls.getName(), e1);
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
    
    private final ZooPCImpl getInstance(ZooClassDef clsDef, long oid, ZooPCImpl co) {
    	if (co != null) {
    		//might be hollow!
    		co.jdoZooMarkClean();
    		return co;
        }
        
		Class<?> cls = clsDef.getJavaClass(); 
    	ZooPCImpl obj = (ZooPCImpl) createInstance(cls);
    	//TODO why not dirty-new?
        obj.jdoZooInit(ObjectState.PERSISTENT_CLEAN, clsDef.getProvidedContext(), oid);
        return obj;
    }

    private Object getGO(long oid, ZooClassDef cls) {
    	if (cls.getClassName().equals(DBHashMap.class.getName()) || 
    			cls.getClassName().equals(DBLargeVector.class.getName()) ||
    			cls.getClassName().equals(DBArrayList.class.getName())) {
    		return getInstance(cls, oid, null);
    	}
    	GOProxy hdl = cache.findOrCreateGo(oid, cls.getVersionProxy());
    	return hdl;
    }
}
