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
package org.zoodb.jdo.internal.util;

import java.util.HashMap;
import java.util.Map;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;
import javax.jdo.spi.PersistenceCapable;

import org.zoodb.jdo.PersistenceManagerImpl;
import org.zoodb.jdo.TransactionImpl;

/**
 * This class serves as a replacement for transient fields in persistent classes that
 * can not be allowed to be garbage collected. It provides <tt>transient</tt>
 * behavior for a <code>static</code> field. <b>Instances of this class must
 * always be referenced via the <tt>static</tt> modifier (see example below).
 * It needs to be <code>static</code> because <code>transient</code> is not 
 * reliable (see below) and because it can't be 'normal' (persistent).</b>
 * <br> 
 * In cases, where the data can not be regenerated, this class
 * can be used to store transient attributes safely for the lifetime
 * of the Java VM.
 * <p>
 * The example below shows hot to use the <code>TransientField</code> class.
 * <br>
 * <code>
 * class Example {<br>
 * &nbsp //A transient field of type "String"<br>
 * &nbsp private final static TransientField&lt;String&gt; _tempName = 
 *       new TransientField&lt;String&gt;("default name");<br>
 * &nbsp private final static TransientField&lt;Boolean&gt; _tempBool = 
 *       new TransientField<Boolean>(true);<br>
 * &nbsp private final static TransientField&lt;Number&gt; _tempNumber = 
 *       new TransientField&lt;Number&gt;();<br>
 * &nbsp <br>
 * &nbsp public String getTempName() {<br>
 * &nbsp &nbsp return _tempName.get(this);<br>
 * &nbsp }<br>
 * &nbsp <br>
 * &nbsp public void setTempName(String name) {<br>
 * &nbsp &nbsp _tempName.set(this, name);<br>
 * &nbsp }<br>
 * &nbsp <br>
 * &nbsp public boolean getTempBoolean() {<br>
 * &nbsp &nbsp return _tempBool.get(this);<br>
 * &nbsp }<br>
 * &nbsp <br>
 * &nbsp public void setTempBoolean(boolean b) {<br>
 * &nbsp &nbsp _tempBool.set(this, b);<br>
 * &nbsp }<br>
 * &nbsp <br>
 * &nbsp public void setTempLong(Long l) {<br>
 * &nbsp &nbsp _tempNumber.set(this, l);<br>
 * &nbsp }<br>
 * &nbsp <br>
 * &nbsp public void setTempDouble(Double d) {<br>
 * &nbsp &nbsp _tempNumber.set(this, d);<br>
 * &nbsp }<br>
 * &nbsp <br>
 * &nbsp public void finalize() {<br>
 * &nbsp &nbsp try {<br>
 * &nbsp &nbsp &nbsp _tempName.cleanIfTransient(this);<br>
 * &nbsp &nbsp &nbsp _tempBool.cleanIfTransient(this);<br>
 * &nbsp &nbsp &nbsp _tempNumber.cleanIfTransient(this);<br>
 * &nbsp &nbsp } finally {<br>
 * &nbsp &nbsp &nbsp super.finalize();<br>
 * &nbsp &nbsp }<br>
 * }<br>  
 * </code>
 * <p>
 * This class is optimized to reference as few owners as possible. It stores
 * owner/value pairs only if the value differs from the default value. If a 
 * value is set to the default value, then the owner/value pair is removed.
 * <p>
 * This class does not require the owner nor the value to be persistent class.
 * <p>
 * Each <code>TransientField</code> has a type and a value. The type is
 * defined via generics <code>&lt;T&gt;</Code> in the declaration. The default 
 * value can be set via the constructor, otherwise it is <tt>null</tt>.
 * <p>
 * Internally in this class, persistent objects are identified via their OID, 
 * transient (all not persistent) objects are identified via their identity in 
 * the Java VM. This means persistent objects are unique with respect to their 
 * ObjectStore, whereas non-persistent objects are shared across the whole Java 
 * VM.<br> 
 * E.g. instances of an persistent Object X can be loaded in different Stores. 
 * The Java VM treats them as separate objects, and they do not share their 
 * values in the transient fields managed by TransientField.
 * <p>
 * The implementation of this class is not very performant due to the way the 
 * <code>ObjectStore</code> is accessed.
 * <p>
 * <b>Garbage Collection</b><br>
 * To allow garbage collection of the values and the owner, please set the
 * values to their default (method 1) or call the cleanup method (method 2). 
 * E.g.:<br>
 * <code>
 * class Example {<br>
 * &nbsp private final static TransientField<String> _tempName = 
 *       new TransientField&lt;String&gt;();//default = null<br>
 * &nbsp <br>
 * &nbsp public void allowGarbageCollectionLaternative1() {<br>
 * &nbsp &nbsp _tempName.set(this, null);<br>
 * &nbsp }<br>
 * &nbsp <br>
 * &nbsp public void allowGarbageCollectionAlternative2() {<br>
 * &nbsp &nbsp _tempName.deregisterOwner(this);<br>
 * &nbsp <br>
 * &nbsp public void finalize() {<br>
 * &nbsp &nbsp try {<br>
 * &nbsp &nbsp &nbsp _tempName.cleanIfTransient(this);<br>
 * &nbsp &nbsp } finally {<br>
 * &nbsp &nbsp &nbsp super.finalize();<br>
 * &nbsp &nbsp }<br>
 * &nbsp }<br>
 * }<br>  
 * </code><br>
 * Otherwise neither the owner nor the value can be garbage collected.
 * 
 * @author  Tilmann Zaeschke
 * @param <T> Type of the fields value.
 */
public class TransientField<T> {

    //A list of all Transient fields
    private static final Map<TransientField<?>, Object> _allFields = 
        new WeakIdentityHashMap<TransientField<?>, Object>(); 
    
    //A map to have one OidMap per Transaction.
    //Each OidMap maps all instances to their transient value.
    //Note that the PersitenceManager may never be garbage collected as long as
    //it is in this list. This is because the persistent objects in the OidMap
    //appear to have a hard reference to the PersistenceManager.
    //We make the map 'weak' anyway.
    private final Map<PersistenceManager, OidMap<Object, T>> _txMap = 
        new WeakIdentityHashMap<PersistenceManager, OidMap<Object, T>>();
    //have a field to avoid garbage collection
    private final OidMap<Object, T> _noTx = new OidMapTrans<Object, T>();
    {
    	_txMap.put(null, _noTx);
    }
    
    private final T _default;

    //The constructors are only called once per class/field, when the 
    //instance of Class object of the referencing class is loaded.

    /**
     * Initialises the field with the given value.
     * @param defaultValue Default value, can be <code>null</code>.
     */
    public TransientField(T defaultValue) {
        super();
        synchronized (_allFields) {
        	_allFields.put(this, null);
        }
        _default = defaultValue;
    }

    private T getValue(Object key) {
        if (key == null) {
            throw new NullPointerException("Invalid value for owner: null");
        }
        PersistenceManager pm = JDOHelper.getPersistenceManager(key);
        //try shortcut
        if (_txMap.containsKey(pm) && _txMap.get(pm).containsKey(key)) {
        	return _txMap.get(pm).get(key, pm);
        }
        
        //Okay, start searching
        //This also treats case where it could be moved from one TX to another.
        for (PersistenceManager pm2: _txMap.keySet()) {
            OidMap<Object, T> om = _txMap.get(pm2);
            if (om.containsKey(key)) {
                if (!pm.equals(pm2)) {  //use != ?
                    //persistent state must have changed!
                    //ensure that _txMap.get(pm) exists!!!
                	if (!_txMap.containsKey(pm)) {
                        _txMap.put(pm, new OidMapPers<Object, T>());
                	}
                    _txMap.get(pm).put(key, om.remove(key, pm2));
                }
                return _txMap.get(pm).get(key, pm);
            }
        }
        
        //okay, doesn't exist, so we return the default
        return _default;
    }

    private final void setValue(Object key, T value) {
        if (key == null) {
            throw new NullPointerException("Invalid value for owner: null");
        }

        //Works e.g. for null:
        if (value == _default) {
        	remove(key);
            return;
        }
        if (value == null || _default == null) {
        	//can't be equal, see previous 'if'.
        	put(key, value);
        	return;
        }
        //TODO remove this if clause
        //Test non-persistent classes with .equals(): String, Long, FineTime.. 
        if (!PersistenceCapable.class.isAssignableFrom(value.getClass())) {
            //check null
            if (value.equals(_default)) {
            	remove(key);
                return;
            }
        }
        put(key, value);
    }

    private void put(Object key, T value) {
        if (key == null) {
            throw new NullPointerException("Invalid value for owner: null");
        }
        PersistenceManager pm = JDOHelper.getPersistenceManager(key);
        
        //ensure that _txMap.get(pm) exists!!!
    	if (!_txMap.containsKey(pm)) {
    		if (pm == null) {
    			throw new IllegalStateException();
    		}
            _txMap.put(pm, new OidMapPers<Object, T>());
    	}

    	//try shortcut
        if (_txMap.get(pm).containsKey(key)) {
        	_txMap.get(pm).put(key, value);
        	return;
        }
        
        //Okay, start searching
        //This also treats cases where it could be moved from one TX to another.
        for (PersistenceManager pm2: _txMap.keySet()) {
            OidMap<Object, T> om = _txMap.get(pm2);
            if (om.containsKey(key)) {
                //persistent state must have changed!
            	om.remove(key, pm2);
            	break;
            }
        }
        //okay, doesn't exist
        _txMap.get(pm).put(key, value);
    }
    
    private void remove(Object key) {
        if (key == null) {
            throw new NullPointerException("Invalid value for owner: null");
        }
        PersistenceManager pm = JDOHelper.getPersistenceManager(key);
        
        //try shortcut
        if (_txMap.containsKey(pm) && _txMap.get(pm).containsKey(key)) {
        	_txMap.get(pm).remove(key, pm);
        	if (_txMap.get(pm).isEmpty() && pm != null) {
        		_txMap.remove(pm);
        	}
        	return;
        }
        
        //Okay, start searching
        //This also treats cases where it could be moved from one TX to another.
        for (PersistenceManager pm2: _txMap.keySet()) {
            OidMap<Object, T> om = _txMap.get(pm2);
            if (om.containsKey(key)) {
            	_txMap.get(pm2).remove(key, pm2);
            	if (_txMap.get(pm2).isEmpty() && pm != null) {
            		_txMap.remove(pm2);
            	}
            	return;
            }
        }
        //okay, doesn't exist
    }
    
    /**
     * Returns the object stored in this field. 
     * <code>null</code> is returned if the stored value is <code>null</code>.
     * @param owner - The owner of this field. 
     * @return Returns the stored object.
     */
    public final synchronized T get(Object owner) {
        return getValue(owner);
    }

    /**
     * This method stores the given value for the given 
     * owner/field combination. 
     * @param owner - The owner of this field. 
     * @param value - The value to set, can be <code>null</code>.
     */
    public final synchronized void set(Object owner, T value) {
        setValue(owner, value);
    }

    /**
     * This command should be called at the end of the owners lifetime to
     * free up associated objects and allow the owner to be garbage collected.
     * <br>
     * If the owner was never registered, this method simply returns.
     * <br>
     * Every owner should call this method for every referenced <code>
     * TransientField</code>.
     * <br>
     * There is no harm in de-registering an object, a following call
     * to <code>get()</code> will return the default value unless <code>
     * set()</code> is used.
     * <br>
     * Another way of de-registering an object is by setting it to its
     * default value, as supplied in the constructor. The object is
     * automatically re-registered when the <code>set(...)</code> method
     * is called with a value different from the default value. 
     * @param owner - The owner of this field. 
     */
    public final synchronized void deregisterOwner(Object owner) {
        //Nothing happens if owner was never registered.
        remove(owner);
    }

    /**
     * Returns <code>null</code> if the object is transient or not 
     * persistent capable.
     * @param obj
     * @return <code>null</code> if the object is transient or not 
     * persistent capable.
     */
    static final Object getObjectId(Object obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        return JDOHelper.getObjectId(obj);
    }

    /**
     * This method frees up all transient fields that are associated with the
     * given Transaction.
     * @param tx
     */
    public static void deregisterTx(Transaction tx) {
        if (tx == null) {
            return;
        }
        if (!(tx instanceof TransactionImpl)) {
            throw new IllegalStateException();
        }
        PersistenceManager pm = tx.getPersistenceManager();
        synchronized (_allFields) {
            for (TransientField<?> f: _allFields.keySet()) {
                //returns null if key does not exist.
                synchronized (f) {
                    if (f._txMap.containsKey(pm)) {
                        f._txMap.remove(pm).clear();
                    }
                }
            }
        }
    }
    
    public static void deregisterPm(PersistenceManager pm) {
        if (pm == null) {
            return;
        }
        if (!(pm instanceof PersistenceManagerImpl)) {
            throw new IllegalStateException();
        }
        synchronized (_allFields) {
            for (TransientField<?> f: _allFields.keySet()) {
                //returns null if key does not exist.
                synchronized (f) {
                    if (f._txMap.containsKey(pm)) {
                        f._txMap.remove(pm).clear();
                    }
                }
            }
        }
    }
    
    /**
     * @param pm
     * @return Number of objects registered with this persistence manager.
     */
    public int size(PersistenceManager pm) {
        if (!_txMap.containsKey(pm)) {
            return 0;
        }
        return _txMap.get(pm).size();
    }

    /**
     * Each TransientField provides one OidMap per PersistenceManager. 
     * Each entry maps one object to it's field's transient value.
     *
     * @author Tilmann Zaeschke
     *
     * @param <K>
     * @param <V>
     */
    private static abstract class OidMap<K, V> {

        abstract int size();
        
        abstract void clear();

        boolean isEmpty() { return size() == 0; }

        abstract V get(Object key, PersistenceManager pm);

        abstract Object put(K key, V remove);

        abstract V remove(Object key, PersistenceManager pm);

        abstract boolean containsKey(Object key);
    }
    
    final static class OidMapPers<K, V> extends OidMap<K, V> {
        //Maps for persistent keys.
        private HashMap<Object, OidMapEntry<V>> _pMap = 
            new HashMap<Object, OidMapEntry<V>>();

        final boolean containsKey(Object key) {
            Object oid = TransientField.getObjectId(key);
            if (oid == null) {
                //Becoming transient: the Object will retain it's OID, so we 
            	//can find previously set values. No need to throw an Excep.
            	//throw new IllegalArgumentException();
            	return false;
            }
            return _pMap.containsKey(oid);
        }

        final V get(Object key, PersistenceManager pm) {
            Object oid = TransientField.getObjectId(key);
            return _pMap.get(oid).getValue(pm);
        }

        final Object put(K key, V value) {
            Object oid = TransientField.getObjectId(key);
            if (oid == null) {
            	throw new IllegalArgumentException();
            }
            return _pMap.put(oid, new OidMapEntry<V>(value));
        }

        final V remove(Object key, PersistenceManager pm) {
            Object oid = TransientField.getObjectId(key);
            if (oid == null) {
            	throw new IllegalArgumentException();
            }
            return _pMap.remove(oid).getValue(pm);
        }

        int size() {
            return _pMap.size();
        }

        @Override
        void clear() {
            _pMap.clear();
        }
    }

    final static class OidMapTrans<K, V> extends OidMap<K, V> {
        //Maps for transient and persistent keys.
        //Allow garbage collection of keys!
        private Map<K, OidMapEntry<V>> _tMap = 
        	new WeakIdentityHashMap<K, OidMapEntry<V>>();

        final boolean containsKey(Object key) {
            return _tMap.containsKey(key);
        }

        final V get(Object key, PersistenceManager pm) {
            return _tMap.get(key).getValue(pm);
        }

        final Object put(K key, V value) {
            return _tMap.put(key, new OidMapEntry<V>(value));
        }

        final V remove(Object key, PersistenceManager pm) {
            if (!_tMap.containsKey(key)) {
                return null;
            }
            return _tMap.remove(key).getValue(pm);
        }

        int size() {
            return _tMap.size();
        }

        @Override
        void clear() {
            _tMap.clear();
        }
    }

    /**
     * OidMapEntries can refer to persistent values or non-persistent values.
     * Non-persistent values should not be garbage collectible.
     * Persistent values should be garbage collectible, but only their OID is
     * stored anyway.
     */
    private final static class OidMapEntry<T> {
        private boolean _isPersistent = false;
        private Object _value;

        OidMapEntry(Object value) {
            if (value == null) {
                _value = null;
                return;
            }
            Object oid = TransientField.getObjectId(value);
            if (oid != null) {
                _value = oid;
                _isPersistent = true;
            } else {
                _value = value;
            }
        }

        @SuppressWarnings("unchecked")
        final T getValue(PersistenceManager pm) {
            if (!_isPersistent) {
                return (T)_value;
            }
            return (T)pm.getObjectById(_value, true);
        }
    }

    /**
     * This method should be called in the <tt>finalize()</tt> method of all
     * classes that use TransientFields.
     * <p>
     * This method cleans up the WeakHashMap that references the transient values
     * of non-persistent objects.Because if the owner is not persistent, then the
     * TransientField should be removed if the object is garbage collected.
     * <p> 
     * It should be noted that contrary to the SUN javadoc, only the keys
     * in a WeakHashMap are garbage collectible. But the Entries & values
     * are <b>not</b> immediately available for garbage collection when the key
     * is removed..
     * Looking at the WeakHashMap code makes this obvious, a bug has been 
     * raised on SUN Java 1.5.0.    
     * @param owner
     */
    public void cleanIfTransient(Object owner) {
    	//We do not check for PM here, because it is not really necessary.
    	_noTx.remove(owner, null);
    }
}
