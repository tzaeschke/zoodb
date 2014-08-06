package org.zoodb.internal.util;

import java.lang.ref.SoftReference;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/*
 *  @(#)HashMap.java    1.73 07/03/13
 *
 * Copyright 2006 Sun Microsystems, Inc. All rights reserved.
 * SUN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

/**
 * This is a modified version of the java.util.HashMap class with three important special features.
 * Like PrimLongMapLI: <p>
 * a) only 'long' are admitted as keys.
 * b) As a) implies, this map does not inherit java.util.Map. This is also done to avoid performance
 *    penalty from polymorphism.
 *    
 * In addition to PrimLongMapLI: <p>
 * c) This map uses internally SoftReferences for all values. This should makes it suitable as 
 *    cache.
 * Strategy:
 * 1) Do not remove gc()'d elements discovered in get(). The get() method is only called for 
 *    checking whether an element is in the cache. If it is not, i.e. if it has been gc()'d, then
 *    the element will be added. Since replacing is cheaper than removing and adding, the get()
 *    method does not remove empty elements.
 * 2) remove() doesn't need special treatment either
 * 3) put() also does not need special treatment
 * 4) values() does get special treatment, because this is where we will remove collected elements.
 *    The values() method is ideal, because it is often called, and because we have to check every 
 *    element anyway, so we get the checking almost for free.
 *    How do we remove elements? There are two ways:
 *    a) Remove while iterating: Probably fastest, because we already look at the elements. But
 *       we need a special remove() method that can not trigger resizing, which would invalidate
 *       the iterator. Also, we need to continuously update the modifyCount in order to allow
 *       out values() iterator to work while invalidating any other iterators().
 *    b) We could create a list of invalid elements. If the values() iterator finishes, it can
 *       remove all elements according to the temporary list.
 *
 * @param <V> the type of mapped values
 *
 * @author  Doug Lea
 * @author  Josh Bloch
 * @author  Arthur van Hoff
 * @author  Neal Gafter
 * @see     java.util.HashMap
 */
public class PrimLongMapLISoft<V> implements PrimLongMap<V> {

    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The load factor used when none specified in constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The table, resized as necessary. Length MUST Always be a power of two.
     */
    private transient Entry<V>[] table;

    /**
     * The number of key-value mappings contained in this map.
     */
    private transient int size;

    /**
     * The next size value at which to resize (capacity * load factor).
     * @serial
     */
    private int threshold;

    /**
     * The load factor for the hash table.
     *
     * @serial
     */
    private final float loadFactor;

    /**
     * The number of times this HashMap has been structurally modified
     * Structural modifications are those that change the number of mappings in
     * the HashMap or otherwise modify its internal structure (e.g.,
     * rehash).  This field is used to make iterators on Collection-views of
     * the HashMap fail-fast.  (See ConcurrentModificationException).
     */
    private transient int modCount;

    /**
     * Constructs an empty <tt>HashMap</tt> with the specified initial
     * capacity and load factor.
     *
     * @param  initialCapacity the initial capacity
     * @param  loadFactor      the load factor
     * @throws IllegalArgumentException if the initial capacity is negative
     *         or the load factor is nonpositive
     */
    @SuppressWarnings("unchecked")
	public PrimLongMapLISoft(int initialCapacity, final float loadFactor) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
        }
        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }
        if (loadFactor <= 0 || Float.isNaN(loadFactor)) {
            throw new IllegalArgumentException("Illegal load factor: " + loadFactor);
        }

        // Find a power of 2 >= initialCapacity
        int capacity = 1;
        while (capacity < initialCapacity) {
            capacity <<= 1;
        }

        this.loadFactor = loadFactor;
        threshold = (int) (capacity * loadFactor);
        table = new Entry[capacity];
    }

    /**
     * Constructs an empty <tt>HashMap</tt> with the specified initial
     * capacity and the default load factor (0.75).
     *
     * @param  initialCapacity the initial capacity.
     * @throws IllegalArgumentException if the initial capacity is negative.
     */
    public PrimLongMapLISoft(final int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Constructs an empty <tt>HashMap</tt> with the default initial capacity
     * (16) and the default load factor (0.75).
     */
    @SuppressWarnings("unchecked")
	public PrimLongMapLISoft() {
        this.loadFactor = DEFAULT_LOAD_FACTOR;
        threshold = (int) (DEFAULT_INITIAL_CAPACITY * DEFAULT_LOAD_FACTOR);
        table = new Entry[DEFAULT_INITIAL_CAPACITY];
    }

    /**
     * Constructs a new <tt>HashMap</tt> with the same mappings as the
     * specified <tt>Map</tt>.  The <tt>HashMap</tt> is created with
     * default load factor (0.75) and an initial capacity sufficient to
     * hold the mappings in the specified <tt>Map</tt>.
     *
     * @param   m the map whose mappings are to be placed in this map
     * @throws  NullPointerException if the specified map is null
     */
    public PrimLongMapLISoft(PrimLongMap<? extends V> m) {
        this(Math.max((int) (m.size() / DEFAULT_LOAD_FACTOR) + 1,
                DEFAULT_INITIAL_CAPACITY), DEFAULT_LOAD_FACTOR);
        putAllForCreate(m);
    }

    // internal utilities

    /**
     * Returns index for hash code h.
     */
    private static int indexFor(long h, int length) {
        return (int)h & (length - 1);
    }

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     *
     * @return <tt>true</tt> if this map contains no key-value mappings
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key==null ? k==null :
     * key.equals(k))}, then this method returns {@code v}; otherwise
     * it returns {@code null}.  (There can be at most one such mapping.)
     *
     * <p>A return value of {@code null} does not <i>necessarily</i>
     * indicate that the map contains no mapping for the key; it's also
     * possible that the map explicitly maps the key to {@code null}.
     * The {@link #containsKey containsKey} operation may be used to
     * distinguish these two cases.
     *
     * @see #put(long, Object)
     */
    @Override
    public V get(long keyBits) {
        for (Entry<V> e = table[indexFor(keyBits, table.length)];
        e != null;
        e = e.next) {
            if (e.key == keyBits) {
                return e.getValue();
            }
        }
        return null;
    }

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the
     * specified key.
     *
     * @param   key   The key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     * key.
     */
    @Override
    public boolean containsKey(long key) {
        return getEntry(key) != null;
    }

    /**
     * Returns the entry associated with the specified key in the
     * HashMap.  Returns null if the HashMap contains no mapping
     * for the key.
     */
    private final Entry<V> getEntry(long keyBits) {
        for (Entry<V> e = table[indexFor(keyBits, table.length)];
        e != null;
        e = e.next) {
            if (e.key == keyBits) {
                return e;
            }
        }
        return null;
    }


    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param keyBits key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>.)
     */
    @Override
    public V put(long keyBits, V value) {
        int i = indexFor(keyBits, table.length);

        //TZ: remove, in case it already exists
        removeEntryForKey(keyBits);
//        for (Entry<V> e = table[i]; e != null; e = e.next) {
//            if (e.key == keyBits) {
//            	return e.setValue(value);
//            }
//        }

        incModCount();
        addEntry(keyBits, value, i);
        return null;
    }

    /**
     * This method is used instead of put by constructors and
     * pseudoconstructors (clone, readObject).  It does not resize the table,
     * check for comodification, etc.  It calls createEntry rather than
     * addEntry.
     */
    private void putForCreate(long key, V value) {
    	if (value == null) {
    		return;
    	}
        int i = indexFor(key, table.length);

        /**
         * Look for preexisting entry for key.  This will never happen for
         * clone or deserialize.  It will only happen for construction if the
         * input Map is a sorted map whose ordering is inconsistent w/ equals.
         */
        //TZ: remove, in case it already exists
        removeEntryForKey(key);
//        for (Entry<V> e = table[i]; e != null; e = e.next) {
//            if (e.key == key) {
//                e.value = valueRef;
//                return;
//            }
//        }

        createEntry(key, value, i);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private void putAllForCreate(final PrimLongMap<? extends V> m) {
        for (final Iterator i = m.entrySet().iterator(); i.hasNext();) {
            final PrimLongMapLISoft.Entry<V> e = (Entry<V>) i.next();
            putForCreate(e.getKey(), e.getValue());
        }
    }

    /**
     * Rehashes the contents of this map into a new array with a
     * larger capacity.  This method is called automatically when the
     * number of keys in this map reaches its threshold.
     *
     * If current capacity is MAXIMUM_CAPACITY, this method does not
     * resize the map, but sets threshold to Integer.MAX_VALUE.
     * This has the effect of preventing future calls.
     *
     * @param newCapacity the new capacity, MUST be a power of two;
     *        must be greater than current capacity unless current
     *        capacity is MAXIMUM_CAPACITY (in which case value
     *        is irrelevant).
     */
    private void resize(int newCapacity) {
        Entry<V>[] oldTable = table;
        int oldCapacity = oldTable.length;
        if (oldCapacity == MAXIMUM_CAPACITY) {
            threshold = Integer.MAX_VALUE;
            return;
        }

        @SuppressWarnings("unchecked")
		Entry<V>[] newTable = new Entry[newCapacity];
        transfer(newTable);
        table = newTable;
        threshold = (int) (newCapacity * loadFactor);
    }

    /**
     * Transfers all entries from current table to newTable.
     */
    private void transfer(Entry<V>[] newTable) {
        Entry<V>[] src = table;
        int newCapacity = newTable.length;
        for (Entry<V> e: src) {
//        for (int j = 0; j < src.length; j++) {
//            Entry<V> e = src[j];
            if (e != null) {
                //src[j] = null;  //TZ: seems unnecessary
                do {
                	//TODO skip gc()'d entries? --> Decrement entry counter.
                    Entry<V> next = e.next;
                    int i = indexFor(e.key, newCapacity);
                    e.next = newTable[i];
                    newTable[i] = e;
                    e = next;
                } while (e != null);
            }
        }
    }

    /**
     * Copies all of the mappings from the specified map to this map.
     * These mappings will replace any mappings that this map had for
     * any of the keys currently in the specified map.
     *
     * @param m mappings to be stored in this map
     * @throws NullPointerException if the specified map is null
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
	public void putAll(PrimLongMap<? extends V> m) {
        int numKeysToBeAdded = m.size();
        if (numKeysToBeAdded == 0) {
            return;
        }

        /*
         * Expand the map if the map if the number of mappings to be added
         * is greater than or equal to threshold.  This is conservative; the
         * obvious condition is (m.size() + size) >= threshold, but this
         * condition could result in a map with twice the appropriate capacity,
         * if the keys to be added overlap with the keys already in this map.
         * By using the conservative calculation, we subject ourself
         * to at most one extra resize.
         */
        if (numKeysToBeAdded > threshold) {
            int targetCapacity = (int) (numKeysToBeAdded / loadFactor + 1);
            if (targetCapacity > MAXIMUM_CAPACITY) {
                targetCapacity = MAXIMUM_CAPACITY;
            }
            int newCapacity = table.length;
            while (newCapacity < targetCapacity) {
                newCapacity <<= 1;
            }
            if (newCapacity > table.length) {
                resize(newCapacity);
            }
        }

        for (final Iterator i = m.entrySet().iterator(); i.hasNext();) {
        	PrimLongMapLISoft.Entry<? extends V> e = (Entry<? extends V>) i.next();
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param  key key whose mapping is to be removed from the map
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>.)
     */
    @Override
    public V remove(long key) {
        Entry<V> e = removeEntryForKey(key);
        return e == null ? null : e.getValue();
    }

    /**
     * Removes and returns the entry associated with the specified key
     * in the HashMap.  Returns null if the HashMap contains no mapping
     * for this key.
     */
    private final Entry<V> removeEntryForKey(long keyBits) {
        int i = indexFor(keyBits, table.length);
        Entry<V> prev = table[i];
        Entry<V> e = prev;

        while (e != null) {
            Entry<V> next = e.next;
            if (e.key == keyBits) {
            	incModCount();
                size--;
                if (prev == e) {
                    table[i] = next;
                } else {
                    prev.next = next;
                }
                return e;
            }
            prev = e;
            e = next;
        }
        //TODO reduce size?
        return e;
    }

    /**
     * Special version of remove for EntrySet.
     */
    private final Entry<V> removeMapping(Entry<V> entry) {
        long keyBits = entry.key;
        int i = indexFor(keyBits, table.length);
        Entry<V> prev = table[i];
        Entry<V> e = prev;

        while (e != null) {
            Entry<V> next = e.next;
            if (e.key == keyBits) {
            	incModCount();
                size--;
                if (prev == e) {
                    table[i] = next;
                } else {
                    prev.next = next;
                }
                return e;
            }
            prev = e;
            e = next;
        }

        return e;
    }

    /**
     * Removes all of the mappings from this map.
     * The map will be empty after this call returns.
     */
    @Override
    public void clear() {
    	incModCount();
        Entry<V>[] tab = table;
        for (int i = 0; i < tab.length; i++) {
            tab[i] = null;
        }
        size = 0;
    }

    /**
     * Returns <tt>true</tt> if this map maps one or more keys to the
     * specified value.
     *
     * @param value value whose presence in this map is to be tested
     * @return <tt>true</tt> if this map maps one or more keys to the
     *         specified value
     */
    @Override
    public boolean containsValue(V value) {
        if (value == null) {
            return containsNullValue();
        }

        Entry<V>[] tab = table;
        for (int i = 0; i < tab.length; i++) {
            for (Entry<V> e = tab[i]; e != null; e = e.next) {
                if (value.equals(e.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Special-case code for containsValue with null argument
     */
    private boolean containsNullValue() {
        Entry<V>[] tab = table;
        for (int i = 0; i < tab.length; i++) {
            for (Entry<V> e = tab[i]; e != null; e = e.next) {
                if (e.getValue() == null) {
                    return true;
                }
            }
        }
        return false;
    }

    public static final class Entry<V> extends SoftReference<V> implements PrimLongEntry<V> {
        private final long key;
        private Entry<V> next;
        
        /**
         * Creates new entry.
         */
        Entry(long k, V v, Entry<V> n) {
        	super(v);
            next = n;
            key = k; 
        }

        @Override
		public final long getKey() {
            return key;
        }

        @Override
        public final V getValue() {
            return get();
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
		public final boolean equals(Object o) {
            if (!(o instanceof Entry)) {
                return false;
            }
            if (this == o) {
            	return true;
            }
            Entry<V> e = (Entry) o;
            long k1 = key;
            long k2 = e.key;
            if (k1 == k2) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public final int hashCode() {
        	V value = get();
            return ((int) key) //hashCode())  -> see hash(...) 
            ^ (value == null ? 0 : value.hashCode());
        }

        @Override
        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }

    /**
     * Adds a new entry with the specified key, value and hash code to
     * the specified bucket.  It is the responsibility of this
     * method to resize the table if appropriate.
     *
     * Subclass overrides this to alter the behavior of put method.
     */
    private void addEntry(long key, V value, int bucketIndex) {
        Entry<V> e = table[bucketIndex];
        table[bucketIndex] = new Entry<V>(key, value, e);
        if (size++ >= threshold) {
            resize(2 * table.length);
        }
    }

    /**
     * Like addEntry except that this version is used when creating entries
     * as part of Map construction or "pseudo-construction" (cloning,
     * deserialization).  This version needn't worry about resizing the table.
     *
     * Subclass overrides this to alter the behavior of HashMap(Map),
     * clone, and readObject.
     */
    private void createEntry(long key, V value, int bucketIndex) {
        Entry<V> e = table[bucketIndex];
        table[bucketIndex] = new Entry<V>(key, value, e);
        size++;
    }

    private abstract class HashIterator<E> implements Iterator<E> {
        private Entry<V> next;  // next entry to return
        private int expectedModCount;   // For fast-fail
        private int index;      // current slot
        private Entry<V> current;   // current entry
        private V hardRef; //hard reference to current value to avoid ever returning null

        HashIterator() {
            expectedModCount = modCount;
            if (size > 0) { // advance to first entry
            	next = new Entry<V>(0, null, null);  //dummy
            	nextEntry();
            }
        }

        @Override
        public final boolean hasNext() {
            return next != null;
        }

        public final boolean hasNextEntry() {
            return next != null;
        }

        final Entry<V> nextEntry() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            Entry<V> e = next;
            if (e == null) {
                throw new NoSuchElementException();
            }

            current = next;
            
        	Entry<V>[] t = table;
            Entry<V> prevE = e;
            e = e.next;
            do {
                //TZ find next non-empty entry
                while (e != null && (hardRef = e.get()) == null) {
                	//remove element
                	prevE.next = e.next;
                	incModCount();
                    size--;
                    expectedModCount++;
                    //proceed
                	prevE = e;
                	e = e.next;
                }
                if (e != null) {
                	break;
                }
                
                //proceed to next position in table?
            	while (e == null && index < t.length) {
                	while ((e = t[index++]) == null && index < t.length) { }
                    while (e != null && (hardRef = e.get()) == null) {
                    	//remove element
                    	table[index-1] = e.next;
                    	incModCount();
                        size--;
                        expectedModCount++;
                        //proceed
                    	e = e.next;
                    }
            	}
            	if (index == t.length) {
            		break;
            	}
            } while (true);
            
            
            next = e;
            return current;
        }

        @Override
        public void remove() {
            if (current == null) {
                throw new IllegalStateException();
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            long k = current.key;
            current = null;
            PrimLongMapLISoft.this.removeEntryForKey(k);
            expectedModCount = modCount;
        }

    }

    public final class ValueIterator extends HashIterator<V> implements PLMValueIterator<V> {
    	@Override
        public V next() {
            return nextEntry().getValue();
        }
    	@Override
        public V nextValue() {
            return nextEntry().getValue();
        }
    }

    private final class KeyIterator extends HashIterator<Long> {
    	@Override
        public Long next() {
            return nextEntry().key;
        }
    }

    private final class EntryIterator extends HashIterator<Entry<V>> {
    	@Override
        public Entry<V> next() {
            return nextEntry();
        }
    }

    // Subclass overrides these to alter behavior of views' iterator() method
    private KeyIterator newKeyIterator()   {
        return new KeyIterator();
    }
    private ValueIterator newValueIterator()   {
        return new ValueIterator();
    }
    private EntryIterator newEntryIterator()   {
        return new EntryIterator();
    }


    // Views

    /**
     * Each of these fields are initialized to contain an instance of the
     * appropriate view the first time this view is requested.  The views are
     * stateless, so there's no reason to create more than one of each.
     */
    private transient volatile Set<Long>        keySet = null;
    private transient volatile PrimLongValues values = null;

    private transient Set<? extends PrimLongEntry<V>> entrySet = null;

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or <tt>addAll</tt>
     * operations.
     */
    @Override
    public Set<Long> keySet() {
        Set<Long> ks = keySet;
        return ks != null ? ks : (keySet = new KeySet());
    }

    private final class KeySet extends AbstractSet<Long> {
        @Override
        public KeyIterator iterator() {
            return newKeyIterator();
        }
        @Override
        public int size() {
            return size;
        }
        @Override
        public boolean contains(Object o) {
            return containsKey((Long) o);
        }
        @Override
        public boolean remove(Object o) {
            return PrimLongMapLISoft.this.removeEntryForKey((Long) o) != null;
        }
        @Override
        public void clear() {
        	PrimLongMapLISoft.this.clear();
        }
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  If the map is
     * modified while an iteration over the collection is in progress
     * (except through the iterator's own <tt>remove</tt> operation),
     * the results of the iteration are undefined.  The collection
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Collection.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt> and <tt>clear</tt> operations.  It does not
     * support the <tt>add</tt> or <tt>addAll</tt> operations.
     */
    @Override
    public PrimLongValues values() {
    	PrimLongValues vs = values;
        return vs != null ? vs : (values = new PrimLongValues());
    }

    public final class PrimLongValues extends AbstractCollection<V> {
        @Override
        public ValueIterator iterator() {
            return newValueIterator();
        }
        @Override
        public int size() {
            return size;
        }
        @SuppressWarnings("unchecked")
        @Override
		public boolean contains(Object o) {
            return containsValue((V) o);
        }
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation, or through the
     * <tt>setValue</tt> operation on a map entry returned by the
     * iterator) the results of the iteration are undefined.  The set
     * supports element removal, which removes the corresponding
     * mapping from the map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt> and
     * <tt>clear</tt> operations.  It does not support the
     * <tt>add</tt> or <tt>addAll</tt> operations.
     *
     * @return a set view of the mappings contained in this map
     */
    @SuppressWarnings("unchecked")
    @Override
	public Set<PrimLongEntry<V>> entrySet() {
        Set<PrimLongEntry<V>> es = (Set<PrimLongMap.PrimLongEntry<V>>) entrySet;
        return (Set<PrimLongEntry<V>>) (es != null ? es : (entrySet = new EntrySet()));
    }

    private final class EntrySet extends AbstractSet<Entry<V>> {
        @Override
        public EntryIterator iterator() {
            return newEntryIterator();
        }
        @SuppressWarnings("unchecked")
        @Override
		public boolean contains(Object o) {
            if (!(o instanceof Entry)) {
                return false;
            }
            Entry<V> e = (Entry<V>) o;
            Entry<V> candidate = getEntry(e.getKey());
            return candidate != null && candidate.equals(e);
        }
        @SuppressWarnings("unchecked")
        @Override
		public boolean remove(Object o) {
            return removeMapping((Entry<V>) o) != null;
        }
        @Override
        public int size() {
            return size;
        }
        @Override
        public void clear() {
        	PrimLongMapLISoft.this.clear();
        }
    }
    
    private final void incModCount() {
    	modCount++;
    }
}
