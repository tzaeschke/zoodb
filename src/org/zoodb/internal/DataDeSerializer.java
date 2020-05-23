/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jdo.ObjectState;
import javax.jdo.listener.LoadCallback;

import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBCollection;
import org.zoodb.api.DBHashMap;
import org.zoodb.api.DBLargeVector;
import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.SerializerTools.PRIMITIVE;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.ObjectReader;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Util;


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

    private ObjectReader in;
    
    //Here is how class information is transmitted:
    //If the class does not exist in the hashMap, then it is added and its 
    //name is written to the stream. Otherwise only the id of the class in the 
    //List in written.
    //The class information is required because it can be any sub-type of the
    //Field type, but the exact type is required for instantiation.
    private final ArrayList<Class<?>> usedClasses = new ArrayList<>(20);

    //Using static concurrent Maps seems to be 30% faster than non-static local maps that are 
    //created again and again.
    private static final ConcurrentHashMap<Class<?>, Constructor<?>> DEFAULT_CONSTRUCTORS =
        new ConcurrentHashMap<>(100);
    
    private final AbstractCache cache;
    private boolean allowGenericObjects = false;
    
    //Cached Sets and Maps
    //The maps and sets are only filled after the keys have been de-serialized. Otherwise 
    //the keys will be inserted with a wrong hash value.
    private final ArrayList<MapValuePair> mapsToFill = new ArrayList<>(5);
    private final ArrayList<SetValuePair> setsToFill = new ArrayList<>(5);
    private static class MapValuePair { 
        final Map<Object, Object> map;
        final Object[] values;
        final Object[] keys;
        public MapValuePair(Map<Object, Object> map, int size) {
            this.map = map;
            this.keys = new Object[size];
            this.values = new Object[size];
        }
    }
    private static class SetValuePair { 
        final Set<Object> set;
        final Object[] values;
        public SetValuePair(Set<Object> set, Object[] values) {
        	this.set = set;
        	this.values = values;
        }
    }
    
    /**
     * Create a new DataDeserializer.
     * @param in Stream to read the data from.
     * @param cache The object cache
     */
    public DataDeSerializer(ObjectReader in, AbstractCache cache) {
        this.in = in;
        this.cache = cache;
   }


	/**
     * This method returns an object that is read from the input 
     * stream.
     * @param page page id
     * @param offs offset in page
     * @param skipIfCached Set 'true' to skip objects that are already in the cache
     * @return The read object.
     */
    public ZooPC readObject(int page, int offs, boolean skipIfCached) {
        long clsOid = in.startReading(page, offs);
        long ts = in.getHeaderTimestamp();

        //Read first object:
        long oid = in.readLong();

        //check cache
        ZooPC pc = cache.findCoByOID(oid);
        if (skipIfCached && pc != null) {
            if (pc.jdoZooIsDeleted() || !pc.jdoZooIsStateHollow()) {
                //isDeleted() are filtered out later.
                return pc;
            }
        }

        ZooClassDef clsDef = cache.getSchema(clsOid);
        ObjectReader or = in;
        boolean isEvolved = false;
        if (clsDef.getNextVersion() != null) {
            isEvolved = true;
            GenericObject go = GenericObject.newInstance(clsDef, oid, false, cache);
            readGOPrivate(go, clsDef);
            clsDef = go.ensureLatestVersion();
            in = go.toStream();
            if (oid != in.readLong()) {
                throw new IllegalStateException();
            }
        }
        
        
        ZooPC pObj = getInstance(clsDef, oid, pc);
        pObj.jdoZooSetTimestamp(ts);

        readObjPrivate(pObj, clsDef);
        in = or;
        if (isEvolved) {
            //force object to be stored again
            //TODO is this necessary?
            pObj.jdoZooMarkDirty();
        }
        return pObj;
    }
    
    
    public GenericObject readGenericObject(int page, int offs) {
    	allowGenericObjects = true;
        long clsOid = in.startReading(page, offs);
        long ts = in.getHeaderTimestamp();
        //Read oid
        long oid = in.readLong();
        ZooClassDef clsDef = cache.getSchema(clsOid);
        
        GenericObject go = cache.getGeneric(oid);
        if (go == null) {
        	go = GenericObject.newInstance(clsDef, oid, false, cache);
        }
        go.setOid(oid);
        go.setClassDefOriginal(clsDef);
        go.jdoZooSetTimestamp(ts);
        readGOPrivate(go, clsDef);
    	allowGenericObjects = false;
    	go.jdoZooMarkClean();
    	return go;
    }
    
    
    private void readGOPrivate(GenericObject pObj, ZooClassDef clsDef) {
    	// read first object (FCO)
        deserializeFieldsGO( pObj, clsDef );
        
        deserializeSpecialGO(pObj, clsDef);
        
        postProcessCollections();
        
        //callback stuff
        //TODO Remove or enable???
//        if (pObj instanceof LoadCallback) {
//        	((LoadCallback)pObj).jdoPostLoad();
//        }
//        pObj.jdoZooGetContext().notifyEvent(pObj, ZooInstanceEvent.LOAD);
        
        pObj.jdoZooMarkClean();
    }
    
    private void deserializeFieldsGO(GenericObject obj, ZooClassDef clsDef) {
        ZooFieldDef f1 = null;
        Object deObj = null;
        try {
            //Read fixed size fields
        	int i = 0;
        	for (ZooFieldDef fd: clsDef.getAllFields()) {
                f1 = fd;
                PRIMITIVE prim = fd.getPrimitiveType();
                if (prim != null) {
                	deObj = deserializePrimitive(prim);
                    obj.setFieldRAW(i, deObj);
                } else if (fd.isFixedSize()) {
                	deObj = deserializeObjectNoSco(fd);
                    obj.setField(fd, deObj);
                }
                i++;
        	}
            //Read variable size fields
        	i = 0;
        	for (ZooFieldDef fd: clsDef.getAllFields()) {
                if (!fd.isFixedSize() || fd.isString()) {
                	f1 = fd;
                   	deObj = deserializeObjectSCO();
                    obj.setFieldRawSCO(i, deObj);
                }
                i++;
        	}
        } catch (IllegalArgumentException | SecurityException e) {
            throw new RuntimeException(e);
        } catch (BinaryDataCorruptedException e) {
            throw new BinaryDataCorruptedException("Corrupted Object: " +
                    Util.getOidAsString(obj) + " " + clsDef + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + clsDef + " F:" + f1 , e);
        }
    }

    public void readObject(ZooPC pc, int page, int offs) {
        long clsOid = in.startReading(page, offs);
        long ts = in.getHeaderTimestamp();
    	
        //Read first object:
    	long oid = in.readLong();
    	if (oid != pc.jdoZooGetOid()) {
    		throw DBLogger.newFatalInternal("OID mismatch: " + oid + " vs " + pc.jdoZooGetOid());
    	}
    	
    	ZooClassDef clsDef = cache.getSchema(clsOid);
    	pc.jdoZooMarkClean();
    	pc.jdoZooSetTimestamp(ts);

    	if (clsDef.getNextVersion() != null) {
    		throw DBLogger.newUser("Object has not been evolved to the latest schema version: " +
    				Util.oidToString(oid));
    	}
    	
        readObjPrivate(pc, clsDef);
    }
    
    
    private void readObjPrivate(ZooPC pObj, ZooClassDef clsDef) {
    	// read first object (FCO)
    	//read fixed size part
        deserializeFields1( pObj, clsDef );
        //read variable size part
        deserializeFields2( pObj, clsDef );
        
        //read special classes
        if (pObj instanceof DBCollection) {
        	deserializeSpecial( pObj );
        }

        postProcessCollections();
        
        if (pObj instanceof LoadCallback) {
        	((LoadCallback)pObj).jdoPostLoad();
        }
        pObj.jdoZooGetContext().notifyEvent(pObj, ZooInstanceEvent.LOAD);
    }
    
    private void postProcessCollections() {
        // Build hashed collections. 
        // We do not add entries to Maps/Sets earlier because need to have 'time'
        // to proper deserialize them. The need to be non-hollow to have the correct hash-codes.
        // They will be come non-hollow at latest when we call the hashcode() function (which 
        // should materialize the object), however we have to delay this to resolve circular
        // dependencies: A having a hashmap that contains B and B having a hashmap that contains A.
        for (SetValuePair sv: setsToFill) {
            sv.set.clear();
            for (int i = 0; i < sv.values.length; i++) {
                sv.set.add(sv.values[i]);
            }
            if (sv.set instanceof ZooPC) {
                ((ZooPC)sv.set).jdoZooMarkClean();
            }
        }
        setsToFill.clear();
        for (MapValuePair mv: mapsToFill) {
            mv.map.clear();
            for (int i = 0; i < mv.values.length; i++) {
                mv.map.put(mv.keys[i], mv.values[i]);
            }
            if (mv.map instanceof ZooPC) {
                ((ZooPC)mv.map).jdoZooMarkClean();
            }
        }
        mapsToFill.clear();
        usedClasses.clear();
    }
    
    private ZooPC getInstance(ZooClassDef clsDef, long oid, ZooPC co) {
    	if (co != null) {
    		//might be hollow!
    		co.jdoZooMarkClean();
    		return co;
        }
        
		Class<?> cls = clsDef.getJavaClass(); 
		if (cls == null) {
			throw DBLogger.newUser("Java class not found: " + clsDef.getClassName());
		}
    	ZooPC obj = (ZooPC) createInstance(cls);
    	prepareObject(obj, oid, false, clsDef);
        return obj;
    }

    private void deserializeFields1(Object obj, ZooClassDef clsDef) {
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
        } catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
            throw new RuntimeException(e);
        } catch (BinaryDataCorruptedException e) {
            throw new BinaryDataCorruptedException("Corrupted Object: oid=" +
                    Util.getOidAsString(obj) + 
                    " " + clsDef + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + clsDef + " F:" + 
                    f1 , e);
        }
    }

    private void deserializeFields2(Object obj, ZooClassDef clsDef) {
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
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Field: " + f1.getType() + " " + f1.getName(), e);
        } catch (IllegalAccessException | SecurityException e) {
            throw new RuntimeException(e);
        } catch (BinaryDataCorruptedException e) {
            throw new BinaryDataCorruptedException("Corrupted Object: " +
                    Util.getOidAsString(obj) + " " + clsDef + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + clsDef + " F:" + f1 , e);
        }
    }

    private Object deserializeSCO(Object obj, Class<?> cls) {
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
        } catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
            throw new RuntimeException(e);
        } catch (BinaryDataCorruptedException e) {
            throw new BinaryDataCorruptedException("Corrupted Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 + " DO: " + (deObj != null ? deObj.getClass() : null), e);
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + cls + " F:" + 
                    f1 , e);
        }
    }

    @SuppressWarnings("unchecked")
    private void deserializeSpecial(Object obj) {
        try {
            //Special treatment for persistent containers.
            //Their data is not stored in (visible) fields.
            if (obj instanceof DBHashMap) {
                deserializeDBHashMap((DBHashMap<Object, Object>) obj);
                ((ZooPC)obj).jdoZooMarkClean();
            } else if (obj instanceof DBLargeVector) {
                deserializeDBList((DBLargeVector<Object>) obj);
                ((ZooPC)obj).jdoZooMarkClean();
            } else if (obj instanceof DBArrayList) {
                deserializeDBList((DBArrayList<Object>) obj);
                ((ZooPC)obj).jdoZooMarkClean();
            }
        } catch (UnsupportedOperationException e) {
            throw new UnsupportedOperationException("Unsupported Object: " +
                    Util.getOidAsString(obj) + " " + obj.getClass(), e);
        }
    }

    private void deserializeSpecialGO(GenericObject obj, ZooClassDef def) {
    	if (def.getClassName().equals(DBHashMap.class.getName())) {
            //Special treatment for persistent containers.
            //Their data is not stored in (visible) fields.
    		HashMap<Object, Object> m = new HashMap<>();
    		obj.setDbCollection(m);
    		deserializeDBHashMap(m);
    	} else if (def.getClassName().equals(DBLargeVector.class.getName())) {
    		ArrayList<Object> l = new ArrayList<>();
    		obj.setDbCollection(l);
    		deserializeDBList(l);
    	} else if (def.getClassName().equals(DBArrayList.class.getName())) {
    		ArrayList<Object> l = new ArrayList<>();
    		obj.setDbCollection(l);
    		deserializeDBList(l);
    	}
    }

    private void deserializePrimitive(Object parent, Field field, PRIMITIVE prim)
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
             
    private Object deserializePrimitive(PRIMITIVE prim)
    throws IllegalArgumentException {
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

    private Object deserializeObjectNoSco(ZooFieldDef def) {
        //read class/null info
        Object cls = readClassInfo();
        if (cls == null) {
        	in.skipRead(def.getLength()-1);
            //reference is null
            return null;
        }

        //read instance data
        if (ZooClassDef.class.isAssignableFrom(cls.getClass())) {
            long oid = in.readLong();

            //Is object already in the database or cache?
            return hollowForOid(oid, (ZooClassDef) cls);
        }
        if (String.class == cls) {
        	in.readLong(); //read and ignore magic number
            return null;
        } else if (Date.class == cls) {
            return new Date(in.readLong());
        }

        throw new IllegalArgumentException("Illegal type: " + def.getName() + ": " + 
                def.getTypeName() + " in class " + def.getDeclaringType().getClassName());
    }

    private Object deserializeObjectSCO() {
        // We keep a separate method for now 
        return deserializeObject();
    }

    /**
     * De-serialize objects. If the object is persistent capable, only it's OID
     * is stored. Otherwise it is serialized and the method is called
     * recursively on all of it's fields.
     * @return De-serialized value.
     */
    private Object deserializeObject() {
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
            return hollowForOid(oid, (ZooClassDef) clsO);
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
            return deserializeMap(cls);
        }
        if (Set.class.isAssignableFrom(cls)) {
            return deserializeSet(cls);
        }
        if (Collection.class.isAssignableFrom(cls)) {
            return deserializeCollection(cls);
        }
        
        // TODO disallow? Allow Serializable/ Externalizable
        return deserializeSCO(createInstance(cls), cls);
    }

    private Object deserializeNumber(PRIMITIVE prim) {
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
    
    private Object deserializeEnum() {
        // read meta data
        Class<?> enumType = (Class<?>) readClassInfo();
        short value = in.readShort();
		return enumType.getEnumConstants()[value];
    }

   private Object deserializeArray() {
        // read meta data
	   	Object innerType = readClassInfo();
	   	if (innerType == null) {
	   		return null;
	   	}
	   	if (ZooClassDef.class.isAssignableFrom(innerType.getClass())) {
		   	if (allowGenericObjects) {
		   		//innerType = GenericObject.class;
		   		innerType = findOrCreateGoClass((ZooClassDef)innerType);
		   		//innerType = Object.class;
		   	} else {
		   		innerType = ((ZooClassDef)innerType).getJavaClass();
		   	}
	   	}
        String innerTypeAcronym = deserializeString();
        
        short dims = in.readShort();
        
        // read data
        return deserializeArrayColumn((Class<?>) innerType, innerTypeAcronym, dims);
    }

    private Object deserializeArrayColumn(Class<?> innerType, String innerAcronym, int dims) {

        //read length
        int l = in.readInt();
        if (l == -1) {
            return null;
        }
        
        Object array;
        
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

        // deserialize actual content
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

    private void deserializeDBHashMap(Map<Object, Object> c) {
        final int size = in.readInt();
        c.clear();
        if (c instanceof DBHashMap) {
        	((DBHashMap<Object, Object>)c).resize(size);
        }
        MapValuePair pair = new MapValuePair(c, size);
        for (int i=0; i < size; i++) {
            //We don't fill the Map here to resolve circular dependencies, see javadoc above.
            pair.keys[i] = deserializeObject();
            pair.values[i] = deserializeObject();
        }
        mapsToFill.add(pair);
    }
    
    private void deserializeDBList(List<Object> c) {
        final int size = in.readInt();
        c.clear();
        if (c instanceof DBArrayList) {
        	((DBArrayList<Object>)c).resize(size);
        }
        for (int i = 0; i < size; i++) {
            c.add(deserializeObject());
        }
    }
    
    @SuppressWarnings("unchecked")
    private Map<Object, Object> deserializeMap(Class<?> cls) {
        int size = in.readInt();
        Map<Object, Object> m = (Map<Object, Object>) createInstance(cls);
        MapValuePair pair = new MapValuePair(m, size);
        for (int i=0; i < size; i++) {
            //We don't fill the Map here to resolve circular dependencies, see javadoc above.
            pair.keys[i] = deserializeObject();
            pair.values[i] = deserializeObject();
        }
        mapsToFill.add(pair);
        return m;
   }
    
    @SuppressWarnings("unchecked")
    private Set<Object> deserializeSet(Class<?> cls) {
        int len = in.readInt();
        Set<Object> s = (Set<Object>) createInstance(cls);
        Object[] values = new Object[len];
        for (int i=0; i < len; i++) {
            //We don't fill the Set here to resolve circular dependencies, see javadoc above.
            values[i] = deserializeObject();
        }
        setsToFill.add(new SetValuePair(s, values));
        return s;
    }
    
    @SuppressWarnings("unchecked")
    private Collection<Object> deserializeCollection(Class<?> cls) {
        // This includes Vector and Queue implementations
        Collection<Object> l = (Collection<Object>) createInstance(cls);
        int len = in.readInt();
        for (int i=0; i < len; i++) {
            l.add(deserializeObject());
        }
        return l;
    }

    private String deserializeString() {
    	return in.readString();
    }

	private Class<?> findOrCreateGoClass(ZooClassDef def) {
		if (def.jdoZooIsDirty()) {
			return GenericObject.class;
		}

		try {
			return Class.forName(def.getClassName());
		} catch (ClassNotFoundException e) {
			if (!allowGenericObjects) {
				throw new BinaryDataCorruptedException(
						"Class not found: " + def.getClassName(), e);
			}
		}
		return GenericObject.class;
	}
	
    private Object readClassInfo() {
    	final byte id = in.readByte();
    	switch (id) {
    	//null-reference
    	case SerializerTools.REF_NULL_ID: return null;
    	case SerializerTools.REF_PERS_ID: {
    		long soid = in.readLong();
    		//Schema Evolution
    		//================
    		//Maybe we need to create an OID->Schema-OID index?
    		//Due to schema evolution, the Schema-OID in serialized references may be out-dated with
    		//respect to the referenced object. Generally, it may be impossible to create a hollow 
    		//object from the OID.
    		//Alternative: look up the schema and create a hollow of the latest version?!?!?     
    		//TODO ZooClassDef def = _cache.getSchemaLatestVersion(soid);
    		ZooClassDef def = cache.getSchema(soid);
//    		if (def.getJavaClass() != null) {
//    			return def.getJavaClass();
//    		}
//    		try {
//    			return Class.forName(def.getClassName());
//    		} catch (ClassNotFoundException e) {
//    			if (!allowGenericObjects) {
//    				throw new BinaryDataCorruptedException(
//    						"Class not found: " + def.getClassName(), e);
//    			}
//    		}
    		return def;
    		//return findOrCreateGoClass(def);
    	}
    	case SerializerTools.REF_ARRAY_ID: {
    		//an array
    		return byte[].class;
    	}
    	case SerializerTools.REF_CUSTOM_CLASS_ID: {
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
    					"Class not found (" + id + "): \"" + cName + "\"");
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
    
    private Object createInstance(Class<?> cls) {
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
                DEFAULT_CONSTRUCTORS.putIfAbsent(cls, c);
            }
            //use the constructor
            return c.newInstance();
        } catch (SecurityException | IllegalArgumentException | InstantiationException | IllegalAccessException |
                InvocationTargetException e1) {
            throw new RuntimeException(e1);
        } catch (NoSuchMethodException e1) {
            throw DBLogger.newFatal("Class requires default constructor (can be private): " + 
            		cls.getName(), e1);
        }
    }
    
    private void prepareObject(ZooPC obj, long oid, boolean hollow, ZooClassDef classDef) {
        if (hollow) {
        	cache.addToCache(obj, classDef, oid, ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL);
        } else {
        	cache.addToCache(obj, classDef, oid, ObjectState.PERSISTENT_CLEAN);
        }
    }
    
    private ZooPC hollowForOid(long oid, ZooClassDef clsDef) {
        if (oid == 0) {
            throw new IllegalArgumentException();
        }
        
        //check cache
    	ZooPC obj = cache.findCoByOID(oid);
        if (obj != null) {
        	//Object already exists
            return obj;
        }
        
        if (allowGenericObjects) {
        	//this instance is only used to return the OID (what about when deserializing arrays?)
        	Class<?> c = findOrCreateGoClass(clsDef);
        	if (c == null || GenericObject.class.isAssignableFrom(c)) {
				obj = GenericObject.newInstance(clsDef, oid, false, cache);
        	} else {
    	        obj = (ZooPC) createInstance(clsDef.getJavaClass());
    	        prepareObject(obj, oid, true, clsDef);
        	}
        } else {
        	//ensure latest version, otherwise there is no Class
        	//TODO instead we should store the schema ID, so that we automatically get the 
        	//     latest version...
           	while (clsDef.getNextVersion() != null) {
        		clsDef = clsDef.getNextVersion();
        	}
           	if (clsDef.getJavaClass() == null) {
				throw DBLogger.newUser("Class has not been fully evolved (" + 
						Util.oidToString(oid) + "): " + clsDef);
			} 
 	        obj = (ZooPC) createInstance(clsDef.getJavaClass());
	        prepareObject(obj, oid, true, clsDef);
        }
        return obj;
    }
}
