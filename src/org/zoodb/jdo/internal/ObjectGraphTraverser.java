package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBLargeVector;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * This class traverses all objects in the Java cache. It looks for new 
 * objects that have not been made persistent and makes them persistent.
 * <p>
 * This class is only public so it can be accessed by the test harness. 
 * Please do not use.
 * <p>
 * WARNING: In this context, ObjectIdentityHashsets should be used with care.
 * Since this kind of Set uses the hashcode of an object, it is likely to
 * load the object into the client, even if when calling contains(). Using such
 * a Set in context of the cache may load all object with an entry in the COD.
 * @author Tilmann Zaeschke
 */
public class ObjectGraphTraverser {

    private static final Logger _LOGGER = 
        Logger.getLogger(ObjectGraphTraverser.class.getPackage().getName());

    private PersistenceManager _pm;

    private static final 
    HashMap<Class<? extends Object>, List<Field>> _seenClasses = 
        new HashMap<Class<? extends Object>, List<Field>>();

    private ObjectIdentitySet<Object> _seenObjects;
    private List<Object> _workList;

    //Fine to use HashSet as long as only Long's are stored.
    //Problem occurs when storing Objects, since .equals() might be 
    //overridden and return true for different objects with same content.
    //private HashSet<Long> _cache;
    private Set<Object> _cache;

    private HashMap<Object, Object> _parents;

    /**
     * This HashSet contains types that are not persistent and that
     * cannot refer to other objects/classes (as a containers can do).
     */
    final static HashSet<Class<?>> SIMPLE_TYPES;
    static {
        SIMPLE_TYPES = new HashSet<Class<?>>();
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
        //more?

        //TODO Class should NOT be used as persistent attribute 
        //(Schema evolution is not??)
        SIMPLE_TYPES.add(Class.class);


        //TODO use PERSISTENT_CONTAINER_TYPES
    }

    /**
     * This HashSet contains types that are not persistent and that
     * cannot refer to other objects/classes (as a containers can do).
     */
    final static HashSet<Class<?>> PERSISTENT_CONTAINER_TYPES;
    static {
    	PERSISTENT_CONTAINER_TYPES = new HashSet<Class<?>>();
    	PERSISTENT_CONTAINER_TYPES.add(DBHashtable.class);
    	PERSISTENT_CONTAINER_TYPES.add(DBVector.class);
    	PERSISTENT_CONTAINER_TYPES.add(DBLargeVector.class);
    }

    
    private long _nObjects = 0;
//  private long _nPreTraverseObjects = 0;
    private long _madePersistent = 0;

    /**
     * This class is only public so it can be accessed by the test harness. 
     * Please do not use.
     * @param pm 
     */
    public ObjectGraphTraverser(PersistenceManager pm, ClientSessionCache cache) {
        //_session = (TransSession) DatabaseTools.getCurrentTransaction();
        //_session = EnhancedTransaction.getCurrentEnhancedTransaction();
        _pm = pm;
        _workList = new LinkedList<Object>();
        
        //We need to copy the Enumeration to a local list, because the enum 
        //might be updated by other operations on the API (?). Still true? TODO
        List <CachedObject> cObjs = cache.getAllObjects();
        //TODO can this be removed?? What is it good for? //ZoodDB (except worklist.add, which is necessary)
        for (CachedObject co: cObjs) {
        	Object o = co.obj;
        	if (!co.isPersistent()) {
        		//ignore if not persistent, e.i. detached, deleted, ...
        		continue;   
        		//_pm.makePersistent(o);  //TODO use a low level/internal function?
        	}
        	//TODO ignore clean objects?
        	_workList.add(o);
//        	Object o = co.obj;
//            //this can sometimes be null
//            if (o != null) {
//                if (o instanceof PersistenceCapableImpl) {
//                    if (!((PersistenceCapableImpl)o).jdoIsPersistent()) {
//                        _pm.makePersistent(o);
//                    }
//                }
//                _workList.add(o);
//            }
        }

        //Save the current cache for later usage. This is used to see 
        //whether an object was already cached or not and needs to be traversed.
        //DELETED objects are not considered. If they reference other objects, 
        //then these others are already persistent (NEW/DIRTY/CLEAN) or they 
        //should not be processed.
        _cache = new ObjectIdentitySet<Object>();
        _cache.addAll(_workList);//getCachedCleanDirtyNewOnly();
        if (_cache.contains(null)) {
            _cache.remove(null);
        }
//      _nPreTraverseObjects = _cache.size();

        _seenObjects = new ObjectIdentitySet<Object>(_workList.size()*2);
        _parents = new HashMap<Object, Object>(_workList.size());
    }

    /**
     * This class is only public so it can be accessed by the test harness. 
     * Please do not use.
     */
    public synchronized final void traverse() {
        //Intention is to find the NEW objects that will become persistent
        //through reachability.
        //For this, we have to check objects that are DIRTY or NEW (by 
        //makePersistent()). 

    	DatabaseLogger.debugPrintln(1, "Starting OGT: " + _workList.size());
        long t1 = System.currentTimeMillis();
        while (!_workList.isEmpty()) {
            _nObjects++;
            Object object = _workList.remove(0);
//TODO remove            try {
                //Objects in the work-list are always already made persistent:
                //Objects in the work-list are either added by the constructor 
                //(already made  persistent).
                //or have been added by addToWorkList (which uses 
                //makePersistent() first).
                if (SIMPLE_TYPES.contains(object.getClass())) {
                    continue;
                }

                if (PERSISTENT_CONTAINER_TYPES.contains(object.getClass())) {
                    doPersistentContainer(object);
                } else if (object instanceof Object[]) {
                    doArray((Object[]) object, object);
                } else if (object instanceof Collection) {
                    doCollection((Collection) object, object);
                } else if (object instanceof Map) {
                    doCollection(((Map) object).keySet(), object);
                    doCollection(((Map) object).values(), object);
                } else if (object instanceof Dictionary) {
                    doEnumeration(((Dictionary) object).keys(), object);
                    doEnumeration(((Dictionary) object).elements(), object);
                } else if (object instanceof Enumeration) {
                    doEnumeration((Enumeration) object, object);
                } else {
                    if (object.getClass().getName().startsWith("com.versant")) {
                        continue;
                    }

                    doObject(object);
                }
//            } catch (VException e) {
//                if (e.getErrno() == 5006) {
//                    Handle h = TransSession.objectToHandle(object);
//                    _LOGGER.warning("Object not found: " + h
//                            + " - " + object.getClass().getName());
//                } else {
//                    throw new StoreException(e);
//                }
//
//            }
        }
        long t2 = System.currentTimeMillis();
        DatabaseLogger.debugPrintln(1, "Finished OGT: " + _nObjects + " (seen="
                + _seenObjects.size() + " ) / " + (t2-t1)/1000.0
                + " MP=" + _madePersistent);    
    }

    /**
     * @return A HashMap containing the (child/parent) pairs.
     */
    public final synchronized HashMap<Object, Object> getParents() {
        return _parents;   
    }

    final private void addToWorkList(Object object, Object parent) {
        if (parent == null || object == null)
            return;

        Class<? extends Object> cls = object.getClass();
        if (SIMPLE_TYPES.contains(cls)) {
            //This can happen when called from doMap(), doContainer(), ...
            return;
        }

        if (PERSISTENT_CONTAINER_TYPES.contains(cls)) {
            _parents.put(object, parent);
        }

        if (object instanceof PersistenceCapableImpl) {
            if (!((PersistenceCapableImpl)object).jdoIsPersistent()) {
                //Make object persistent, if necessary
                _pm.makePersistent(object);
                _cache.add(object);
                _madePersistent++;
            } else if (!_cache.contains(object)) {
                //This avoids loading more objects from the database.
                //Persistent objects that haven't been in the cache are not
                //interesting for traversal, they will never lead to other 
                //new objects.
                return;
            }
        }

        if (!_seenObjects.contains(object)) {
            _workList.add(object);
            _seenObjects.add(object);
        }
    }

    final private void doArray(Object[] array, Object parent) {
        for (Object o: array) {
            addToWorkList(o, parent);
        }
    }

    final private void doEnumeration(Enumeration enumeration, Object parent) {
        while (enumeration.hasMoreElements()) {
            addToWorkList(enumeration.nextElement(), parent);
        }
    }

    final private void doCollection(Collection<?> col, Object parent) {
        for (Object o: col) {
            addToWorkList(o, parent);
        }
    }

    private final void doObject(Object parent) {			
        for (Field field: getFields(parent.getClass())) {
            try {
                //add the value to the working list
                addToWorkList(field.get(parent), parent);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(
                        "Unable to access field \"" + field.getName()
                        + "\" of class \"" + parent.getClass().getName()
                        + "\" from OGT.", e);
            }
        }
    }

    /** 
     * Handles com.versant.util.VVector, com.versant.util.DVector,
     * com.versant.util.LargeVector
     */ 
    private final void doPersistentContainer(Object container) {
        if (container instanceof DBVector) {
            doCollection((DBVector)container, container);
        } else if (container instanceof DBLargeVector) {
            doEnumeration(((DBLargeVector)container).elements(), container);
        } else if (container instanceof DBHashtable) {
            DBHashtable t = (DBHashtable)container;
            doCollection(t.keySet(), container);
            doCollection(t.values(), container);
        } else {
            _LOGGER.severe("WARNING: Objects of this type cannot be " +
                        "distributed or propagated: " + container.getClass());
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

        String type = field.getType().getName();
        int dim = 0;
        while (type.startsWith("[")) {
            dim++;
            type = type.substring(1);
        }
        if (dim == 0) {
            return SIMPLE_TYPES.contains(getClassForName(type));
        } 

        char t = type.charAt(0);
        if (t == 'L') {
            type = type.substring(1).replaceAll(";","");
            return SIMPLE_TYPES.contains(getClassForName(type));
        }

        if ((t == 'B') || (t == 'C') || (t == 'D') || (t == 'F') 
                || (t == 'I') || (t == 'J') || (t == 'S') || (t == 'Z')) {
            return true;
        }

        _LOGGER.severe(
                "Error parsing class \"" + field.getDeclaringClass() + "\"");
        _LOGGER.severe("Unknow field type '" + t + "'in field '" + field + 
                "' (" + field.getType() + ")");
        throw new IllegalArgumentException("Unknow field type '" + t
                + "'in field '" + field + "' (" + field.getType() + ")");
    }

    private static Class<?> getClassForName(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Returns a List containing all of the Field objects for the given class.
     * The fields include all public and private fields from the given class 
     * and its super classes.
     *
     * @param c Class object
     * @return Returns list of interesting fields
     */
    private static final List<Field> getFields (Class<? extends Object> cls) {
        if (_seenClasses.containsKey(cls)) {
            return _seenClasses.get(cls);
        }

        if (cls == Class.class) {
            //Fields of type Class can not be allowed! -> schema evolution!
            throw new IllegalArgumentException(
                    "Encountered object of typ 'Class'");
        }

        List<Field> ret = new ArrayList<Field>();
        while (cls != Object.class) {
            for (Field f: cls.getDeclaredFields ()) {
//              Database.debugPrint(2, "## "+f.getType().getName()
//              +"   "+f.getName());
                if (!SIMPLE_TYPES.contains(f.getType()) && !isSimpleType(f)) {
                    ret.add(f);
                    f.setAccessible(true);
//                  Database.debugPrint(2, "# "
//                  +f.getType().getName()+"   "+f.getName());
                }
            }
            cls = cls.getSuperclass (); //reflection
        }
        _seenClasses.put(cls, ret);
        return ret;
    }
}