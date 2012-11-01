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
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.PersistenceManager;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.DBArrayList;
import org.zoodb.jdo.api.DBCollection;
import org.zoodb.jdo.api.DBHashMap;
import org.zoodb.jdo.api.DBLargeVector;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.internal.util.DatabaseLogger;
import org.zoodb.jdo.internal.util.ObjectIdentitySet;

/**
 * This class traverses all objects in the Java cache. It looks for new 
 * objects that have not been made persistent and makes them persistent.
 * <p>
 * This class is only public so it can be accessed by the test harness. 
 * Please do not use.
 * <p>
 * WARNING: In this context, ObjectIdentityHashsets should be used with care.
 * Since this kind of Set uses the hashcode of an object, it is likely to
 * load the object into the client, even when calling contains(). Using such
 * a Set in context of the cache may result in loading hollow objects as well.
 * 
 * @author Tilmann Zaeschke
 */
public class ObjectGraphTraverser {

    private final PersistenceManager pm;
    private final ClientSessionCache cache;

    //TODO use ZooClassDef from cached object instead? AVoids 'static' modifier!
    private static final IdentityHashMap<Class<? extends Object>, Field[]> SEEN_CLASSES = 
        new IdentityHashMap<Class<? extends Object>, Field[]>();

    private final ObjectIdentitySet<Object> seenObjects;
    private final ArrayList<Object> workList;
    private boolean isTraversingCache = false;
    private int mpCount = 0;
    private final ArrayList<ZooPCImpl> toBecomePersistent;

    /**
     * This HashSet contains types that are not persistent and that
     * cannot refer to other objects/classes (as a containers can do).
     */
    private final static ObjectIdentitySet<Class<?>> SIMPLE_TYPES;
    static {
        SIMPLE_TYPES = new ObjectIdentitySet<Class<?>>();
        SIMPLE_TYPES.add(Boolean.class);
        SIMPLE_TYPES.add(Byte.class);
        SIMPLE_TYPES.add(Character.class);
        SIMPLE_TYPES.add(Double.class);
        SIMPLE_TYPES.add(Float.class);
        SIMPLE_TYPES.add(Integer.class);
        SIMPLE_TYPES.add(Long.class);
        SIMPLE_TYPES.add(Short.class);
        SIMPLE_TYPES.add(Boolean.TYPE);
        SIMPLE_TYPES.add(Byte.TYPE);
        SIMPLE_TYPES.add(Character.TYPE);
        SIMPLE_TYPES.add(Double.TYPE);
        SIMPLE_TYPES.add(Float.TYPE);
        SIMPLE_TYPES.add(Integer.TYPE);
        SIMPLE_TYPES.add(Long.TYPE);
        SIMPLE_TYPES.add(Short.TYPE);
        SIMPLE_TYPES.add(BigDecimal.class);
        SIMPLE_TYPES.add(BigInteger.class);
        SIMPLE_TYPES.add(String.class);
        SIMPLE_TYPES.add(URI.class);
        SIMPLE_TYPES.add(URL.class);
        //more?

        //TODO Class should NOT be used as persistent attribute 
        //(Schema evolution is not??)
        SIMPLE_TYPES.add(Class.class);


        //TODO use PERSISTENT_CONTAINER_TYPES
    }

    /**
     * This class is only public so it can be accessed by the test harness. 
     * Please do not use.
     * @param pm 
     */
    public ObjectGraphTraverser(PersistenceManager pm, ClientSessionCache cache) {
        this.pm = pm;
        this.cache = cache;
        
        //We need to copy the cache to a local list, because the cache we might make additional
        //objects persistent while iterating. We need to ensure that these new objects are covered
        //as well. And we have the problem of concurrent updates in the cache.
        
        workList = new ArrayList<Object>();
        seenObjects = new ObjectIdentitySet<Object>();
        //TODO ObjIdentSet? -> ArrayList might be faster in most cases
        toBecomePersistent = new ArrayList<ZooPCImpl>(); 
    }

    /**
     * This class is only public so it can be accessed by the test harness. 
     * Please do not use.
     */
    public final void traverse() {
        //Intention is to find the NEW objects that will become persistent
        //through reachability.
        //For this, we have to check objects that are DIRTY or NEW (by 
        //makePersistent()). 
    	DatabaseLogger.debugPrintln(1, "Starting OGT: " + workList.size());
        long t1 = System.currentTimeMillis();
        long nObjects = 0;

        nObjects += traverseCache();
        nObjects += traverseWorkList();
                
        long t2 = System.currentTimeMillis();
        DatabaseLogger.debugPrintln(1, "Finished OGT: " + nObjects + " (seen="
                + seenObjects.size() + " ) / " + (t2-t1)/1000.0
                + " MP=" + mpCount);    
    }
    
    private int traverseCache() {
    	isTraversingCache = true;
    	int nObjects = 0;
    	//TODO is this really necessary? Looks VERY ugly.
    	//Profiling FlatObject.commit(): ~7% spent in next()!
    	for (ZooPCImpl co: cache.getDirtyObjects()) {
        	//ignore clean objects. Ignore hollow objects? Don't follow deleted objects.
        	//we require objects that are dirty or new (=dirty and not deleted?)
        	if (co.jdoZooIsDirty() & !co.jdoZooIsDeleted()) {
        		traverseObject(co);
        		nObjects++;
        	}
        }
        isTraversingCache = false;

        //make objects persistent. This has to be delayed after traversing the cache to avoid
        //concurrent modification on the cache.
        for (ZooPCImpl pc: toBecomePersistent) {
        	if (!pc.jdoZooIsPersistent()) {
        		cache.getSession().makePersistent(pc);
        	}
        }
        mpCount += toBecomePersistent.size();
        
        return nObjects;
    }
    
    private int traverseWorkList() {
    	int nObjects = 0;
        while (!workList.isEmpty()) {
            nObjects++;
            Object object = workList.remove(workList.size()-1);
            //Objects in the work-list are always already made persistent:
            //Objects in the work-list are either added by the constructor 
            //(already made  persistent).
            //or have been added by addToWorkList (which uses makePersistent() first).
            traverseObject(object);
        }
        return nObjects;
    }

    @SuppressWarnings("rawtypes")
	private void traverseObject(Object object) {
        if (object instanceof DBCollection) {
            doPersistentContainer(object);
        } else if (object instanceof Object[]) {
            doArray((Object[]) object);
        } else if (object instanceof Collection) {
            doCollection((Collection) object);
        } else if (object instanceof Map) {
            doCollection(((Map) object).keySet());
            doCollection(((Map) object).values());
        } else if (object instanceof Dictionary) {
            doEnumeration(((Dictionary) object).keys());
            doEnumeration(((Dictionary) object).elements());
        } else if (object instanceof Enumeration) {
            doEnumeration((Enumeration) object);
        } else {
            doObject(object);
        }
    }
    
    final private void addToWorkList(Object object) {
        if (object == null)
            return;

        Class<? extends Object> cls = object.getClass();
        if (SIMPLE_TYPES.contains(cls)) {
        	//TODO FIX? zoodb
            //This can happen when called from doMap(), doContainer(), ...
            return;
        }

        if (object instanceof ZooPCImpl) {
        	ZooPCImpl pc = (ZooPCImpl) object;
        	//This can happen if e.g. a LinkedList contains new persistent capable objects.
            if (!pc.jdoZooIsPersistent()) {
                //Make object persistent, if necessary
            	if (isTraversingCache) {
            		//during cache traversal:
            		toBecomePersistent.add(pc);
            	} else {
            		//during work list traversal:
            		pm.makePersistent(pc);
            		mpCount++;
            	}
            } else {
            	//This object is already persistent. It is either in the worklist or it is 
            	//uninteresting (not dirty).
            	return;
            }
        }

        if (!seenObjects.contains(object)) {
            workList.add(object);
            seenObjects.add(object);
        }
    }

    final private void doArray(Object[] array) {
        for (Object o: array) {
            addToWorkList(o);
        }
    }

    @SuppressWarnings("rawtypes")
	final private void doEnumeration(Enumeration enumeration) {
        while (enumeration.hasMoreElements()) {
            addToWorkList(enumeration.nextElement());
        }
    }

    final private void doCollection(Collection<?> col) {
        for (Object o: col) {
            addToWorkList(o);
        }
    }

    private final void doObject(Object parent) {			
        for (Field field: getFields(parent.getClass())) {
            try {
                //add the value to the working list
                addToWorkList(field.get(parent));
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(
                        "Unable to access field \"" + field.getName()
                        + "\" of class \"" + parent.getClass().getName()
                        + "\" from OGT.", e);
            }
        }
    }

    /** 
     * Handles persistent Collection classes.
     */ 
    @SuppressWarnings("rawtypes")
	private final void doPersistentContainer(Object container) {
        if (container instanceof DBArrayList) {
            doCollection((DBArrayList)container);
        } else if (container instanceof DBLargeVector) {
//            doEnumeration(((DBLargeVector)container).elements(), container);
            doCollection(((DBLargeVector)container));
        } else if (container instanceof DBHashMap) {
            DBHashMap t = (DBHashMap)container;
            doCollection(t.keySet());
            doCollection(t.values());
        } else {
            throw new IllegalArgumentException(
                    "Unrecognized persistent container in OGT: "
                    + container.getClass());
        }
    }

    private static final boolean isSimpleType(Field field) {
        int mod = field.getModifiers();
        if (Modifier.isStatic(mod) || Modifier.isTransient(mod)) {
            return true;
        }

        Class<?> cls = field.getType();
        while (cls.isArray()) {
        	cls = cls.getComponentType();
        }
        return SIMPLE_TYPES.contains(cls);

    }

    /**
     * Returns a List containing all of the Field objects for the given class.
     * The fields include all public and private fields from the given class 
     * and its super classes.
     *
     * @param c Class object
     * @return Returns list of interesting fields
     */
    private static final Field[] getFields (Class<? extends Object> cls) {
        if (SEEN_CLASSES.containsKey(cls)) {
            return SEEN_CLASSES.get(cls);
        }

        if (cls == Class.class) {
            //Fields of type Class can not be allowed! -> schema evolution!
            throw new IllegalArgumentException("Encountered object of type 'Class'");
        }

        List<Field> retL = new ArrayList<Field>();
        for (Field f: cls.getDeclaredFields ()) {
        	if (!isSimpleType(f)) {
        		retL.add(f);
        		f.setAccessible(true);
        	}
        }

        if (cls.getSuperclass() != Object.class) {
        	for (Field f: getFields(cls.getSuperclass())) {
        		retL.add(f);
        	}
        }
        Field[] ret = retL.toArray(new Field[retL.size()]);
        SEEN_CLASSES.put(cls, ret);
        return ret;
    }
}