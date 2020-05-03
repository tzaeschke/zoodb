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
package org.zoodb.jdo.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;

/**
 * This class represents an extent that is backed by a Java collection.
 * 
 * @author Tilmann Zaeschke
 */
public class CollectionExtent implements Extent<ZooPC> {
    
    private final Class<ZooPC> extClass;
    private final boolean subclasses;
    private final ArrayList<CloseableIterator<ZooPC>> allIterators = 
        new ArrayList<CloseableIterator<ZooPC>>();
    private final PersistenceManagerImpl pm;
    private final Collection<ZooPC> coll;
    
    /**
     * 
     * @param c The wrapped collection
     * @param pm The PersistenceManager
     * @param subClasses whether sub-classes should be returned
     */
    @SuppressWarnings("unchecked")
	public CollectionExtent(Collection<?> c, PersistenceManagerImpl pm, boolean subClasses) {
        this.pm = pm;
    	this.subclasses = subClasses;
    	this.coll = (Collection<ZooPC>) c;
		if (c.isEmpty()) {
			extClass = ZooPC.class;
			return;
		}
		Iterator<?> iter = c.iterator();
		Object o1 = iter.next();
		Class<?> cls = o1.getClass();
    	if (!ZooPC.class.isAssignableFrom(cls)) {
    		throw DBLogger.newUser("Class is not persistence capable: " + cls.getName());
    	}
    	if (pm != JDOHelper.getPersistenceManager(o1)) {
    		throw DBLogger.newUser("The object belongs to another PersistenceManager");
    	}
    	while (iter.hasNext()) {
    		Object o2 = iter.next();
    		Class<?> cls2 = o2.getClass();
        	if (!ZooPC.class.isAssignableFrom(cls2)) {
        		throw DBLogger.newUser("Class is not persistence capable: " + cls.getName());
        	}
        	if (pm != JDOHelper.getPersistenceManager(o1)) {
        		throw DBLogger.newUser("The object belongs to another PersistenceManager");
        	}
    		while (!cls.isAssignableFrom(cls2)) {
    			cls = cls.getSuperclass();
    		}
    	}
		extClass  = (Class<ZooPC>) cls;
    }

    /**
     * @see Extent#iterator()
     */
    @Override
	public Iterator<ZooPC> iterator() {
    	Iterator<ZooPC> it = coll.iterator();
    	if (it instanceof CloseableIterator) {
    		allIterators.add((CloseableIterator<ZooPC>) it);
    	}
    	return it;
    }

    /**
     * @see Extent#close(java.util.Iterator)
     */
    @Override
	public void close(Iterator<ZooPC> i) {
        if (i instanceof CloseableIterator) {
        	CloseableIterator.class.cast(i).close();
            allIterators.remove(i);
        }
    }

    /**
     * @see Extent#closeAll()
     */
    @Override
	public void closeAll() {
        for (CloseableIterator<ZooPC> i: allIterators) {
            i.close();
        }
        allIterators.clear();
    }

    /**
     * @see Extent#hasSubclasses()
     */
    @Override
	public boolean hasSubclasses() {
        return subclasses;
    }

    /**
     * @see Extent#getPersistenceManager()
     */
    @Override
	public PersistenceManager getPersistenceManager() {
        return pm;
    }
    
	@Override
	public Class<ZooPC> getCandidateClass() {
		return extClass;
	}

	@Override
	public FetchPlan getFetchPlan() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
}
