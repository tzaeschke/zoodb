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
package org.zoodb.internal.util;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This class is an implementation of the Set interface. To check object 
 * equality, it uses '==' instead of 'equals()'. 
 * 
 * @param <E> The type of the elements
 * @author Tilmann Zaeschke
 */
public final class ObjectIdentitySet<E> implements Set<E>
{
    private ObjectIdentitySetEntry<E>[] table;
    private int count;
    private int threshold;

    private static final float LOAD_FACTOR = 0.75f;
    private static final int   INITIAL_CAPACITY = 127;

    /**
     * Create a new ObjectIdentitySet.
     */
    public ObjectIdentitySet () {
        this (INITIAL_CAPACITY);
    }

    @SuppressWarnings("unchecked")
	public ObjectIdentitySet (int initial_capacity) {
        if (initial_capacity > INITIAL_CAPACITY) {
            //First get a good number: (2^n)-1 > initial_capacity
            //Ideally, n would be a prime, but that's getting too 
            //difficult here.
            int p = 1;
            while (initial_capacity > 1) {
                initial_capacity = initial_capacity >> 2;
                p++;
            }
            if (p%2 == 0) {
                p++;
            }
            initial_capacity = (int) Math.pow(2, p) - 1;
        } else {
            //Go for minimal feasible size (expecting resizing)
            initial_capacity = INITIAL_CAPACITY;
        }

        int initial_size = (int) (initial_capacity / LOAD_FACTOR);
        table            = new ObjectIdentitySetEntry [initial_size];
        count            = 0;
        threshold        = initial_capacity;
    }

    /**
     * @param object The object to add
     * @return Returns <tt>true</tt> if the object was added, otherwise 
     * <tt>false</tt>.
     * @see java.util.Set#add(java.lang.Object)
     */
    @Override
	public final boolean add (E object) {
        int hash  = System.identityHashCode (object);
        int index = (hash & 0x7FFFFFFF) % table.length;

        ObjectIdentitySetEntry<E> head = table [index];
        for (ObjectIdentitySetEntry<E> e = head; e != null; e = e.next) {
            if (object == e.object)
                return false;
        }
        table [index] = new ObjectIdentitySetEntry<E> (object, head);

        count++;
        if (count >= threshold)
            rehash (count * 2 + 1);
        return true;
    }

    final void add (E[] objects) {
        for (E obj: objects) {
            add (obj);
        }
    }

    /**
     * @see java.util.Set#contains(java.lang.Object)
     */
    @Override
	public final boolean contains (Object object) {
        int hash  = System.identityHashCode (object);
        int index = (hash & 0x7FFFFFFF) % table.length;

        ObjectIdentitySetEntry<E> e;
        for (e = table [index]; e != null; e = e.next) {
            if (object == e.object)
                return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
	final void rehash (int new_length) {
        ObjectIdentitySetEntry<E>[] old_table = table;
        int old_length = old_table.length;
        //int new_length = old_length * 2 + 1;
        ObjectIdentitySetEntry<E>[] new_table =
            new ObjectIdentitySetEntry [new_length];

        for (int i = 0; i < old_length; i++) {
            ObjectIdentitySetEntry<E> next;
            ObjectIdentitySetEntry<E> e;
            for (e = old_table [i]; e != null; e = next) {
                int hash      = System.identityHashCode (e.object);
                int new_index = ((hash & 0x7FFFFFFF) % new_length);
                next = e.next;
                e.next = new_table [new_index];
                new_table [new_index] = e;
            }
        }

        table     = new_table;
        threshold = (int) (new_length * LOAD_FACTOR);
    }

    /**
     * @see java.util.Set#size()
     */
    @Override
	public final int size() {
        return count;
    }

    /**
     * @see java.util.Set#clear()
     */
    @Override
	@SuppressWarnings("unchecked")
	public final void clear() {
        threshold = INITIAL_CAPACITY;
        count = 0;
        table = new ObjectIdentitySetEntry [(int) (threshold / LOAD_FACTOR)];
    }

    /**
     * @see java.util.Set#isEmpty()
     */
    @Override
	public final boolean isEmpty() {
        return count == 0;
    }

    /**
     * @see java.util.Set#toArray()
     */
    @Override
	public final Object[] toArray() {
        Object[] a = new Object[count];
        int j = 0;
        for (Iterator<E> i = this.iterator(); i.hasNext(); ) {
            a[j++] = i.next();
        }
        return a;
    }

    /**
     * @see java.util.Set#remove(java.lang.Object)
     */
    @Override
	public final boolean remove(Object obj) {
        int hash  = System.identityHashCode (obj);
        int index = (hash & 0x7FFFFFFF) % table.length;

        ObjectIdentitySetEntry<E> e;
        ObjectIdentitySetEntry<E> prev = null;
        for (e = table [index]; e != null; e = e.next) {
            if (obj == e.object) {
                //delete it
                if (prev == null) {
                    //first element
                    table [index] = e.next;
                } else {
                    prev.next = e.next;
                }

                count--;
                // rehash if < 25% filled and size > 127
                if (count < threshold*(1-LOAD_FACTOR) 
                        && count > INITIAL_CAPACITY)
                    rehash (count / 2 + 1);
                return true;
            }
            prev = e;
        }
        return false;
    }

    /**
     * @param c The collection whose elements should be added
     * @see java.util.Set#addAll(java.util.Collection)
     * @return {@code true} if any elements were added
     */
    @Override
	public final boolean addAll(Collection<? extends E> c) {
    	boolean ret = false;
        for (E o: c) {
        	ret |= add(o);
        }
        return ret;
    }

    /**
     * Add all elements of in the enumeration
     * @param en The enumeration whose elements should be added
     */
    public final void addAll(Enumeration<E> en) {
        while (en.hasMoreElements()) {
            add(en.nextElement());
        }
    }

    /**
     * @param c collection to be checked for containment in this set
     * @return {@code true} if this set contains all of the elements of the specified collection
     * @see java.util.Set#containsAll(java.util.Collection)
     */
    @Override
	public final boolean containsAll(Collection<?> c) {
        if (c == null) {
            throw new NullPointerException();
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see java.util.Set#removeAll(java.util.Collection)
     */
    @Override
	public final boolean removeAll(Collection<?> c) {
        if (c == null) {
            throw new NullPointerException();
        }
        boolean hasChanged = false;
        for (Object o: c) {
            if (remove(o)) hasChanged = true;
        }
        return hasChanged;
    }

    /**
     * @see java.util.Set#retainAll(java.util.Collection)
     */
    @Override
	public final boolean retainAll(Collection<?> c) {
        if (c == null) {
            throw new NullPointerException();
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see java.util.Set#iterator()
     */
    @Override
	public final Iterator<E> iterator() {
        return new ObjectIdentitySetIterator();
    }

    /**
     * @param a The array that should be attempted to be filled up
     * @param <T> The type of the elements
     * @return Array representation of this container
     * @see java.util.Set#toArray(Object[])
     */
    @Override
	public final <T> T[] toArray(T[] a) {
        if (a == null) {
            throw new NullPointerException();
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
	public final String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("ObjectIdentitySet(").append(count).append("): ");
        for (Iterator<E> i = this.iterator(); i.hasNext(); ) {
            buf.append(i.next()).append("; ");
        }
        return buf.toString();
    }
    
    
    private final static class ObjectIdentitySetEntry<T> {
        private final T object;
        private ObjectIdentitySetEntry<T> next;

        ObjectIdentitySetEntry (T object1, ObjectIdentitySetEntry<T> next1) {
            object = object1;
            next   = next1;
        }
    }

    private final class ObjectIdentitySetIterator implements Iterator<E> {
        private int pos = 0;
        private ObjectIdentitySetEntry<E> next = null; 

        /**
         * @see java.util.Iterator#remove()
         */
        @Override
		public final void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * @see java.util.Iterator#hasNext()
         */
        @Override
		public final boolean hasNext() {
            if (next == null) {
                for( ; pos < table.length; pos++) {
                    if (table[pos] != null) {
                        next = table[pos];
                        pos++;
                        return true;
                    }
                }
                return false;
            }
            return true;
        }

        /**
         * @see java.util.Iterator#next()
         */
        @Override
		public final E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ObjectIdentitySetEntry<E> n = next;
            next = next.next;
            return n.object;
        }
    }

}

