/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
     * @param c
     * @param pm
     * @param subClasses
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
    		throw DBLogger.newUser("Class is not persistence capabale: " + cls.getName());
    	}
    	if (pm != JDOHelper.getPersistenceManager(o1)) {
    		throw DBLogger.newUser("The object belongs to another PersistenceManager");
    	}
    	while (iter.hasNext()) {
    		Object o2 = iter.next();
    		Class<?> cls2 = o2.getClass();
        	if (!ZooPC.class.isAssignableFrom(cls2)) {
        		throw DBLogger.newUser("Class is not persistence capabale: " + cls.getName());
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
