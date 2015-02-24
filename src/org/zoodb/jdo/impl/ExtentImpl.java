/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.MergingIterator;
import org.zoodb.internal.util.SynchronizedROIteratorC;

/**
 * This class implements JDO behavior for the class Extent.
 * @param <T>
 * 
 * @author Tilmann Zaeschke
 */
public class ExtentImpl<T> implements Extent<T> {
    
    private final Class<T> extClass;
    private final boolean subclasses;
    private final ArrayList<SynchronizedROIteratorC<T>> allIterators = 
        new ArrayList<SynchronizedROIteratorC<T>>();
    private final PersistenceManagerImpl pm;
    private final boolean ignoreCache;
    //This is used for aut-create schema mode, where a persistent class may not be in the database.
    private boolean isDummyExtent = false;
    
    /**
     * @param pcClass
     * @param subclasses
     * @param pm
     */
    public ExtentImpl(Class<T> pcClass, 
            boolean subclasses, PersistenceManagerImpl pm, boolean ignoreCache) {
    	if (!ZooPC.class.isAssignableFrom(pcClass)) {
    		throw new JDOUserException("Class is not persistence capabale: " + 
    				pcClass.getName());
    	}
    	if (pm.getSession().schema().getClass(pcClass) == null) {
    		if (pm.getSession().getConfig().getAutoCreateSchema()) {
    			isDummyExtent = true;
    		} else {
    			throw new JDOUserException("Class schema not defined: " + pcClass.getName());
    		}
    	}
        this.extClass = pcClass;
        this.subclasses = subclasses;
        this.pm = pm;
        this.ignoreCache = ignoreCache;
    }

    /**
     * @see Extent#iterator()
     */
    @Override
	public Iterator<T> iterator() {
    	if (isDummyExtent) {
    		return new MergingIterator<T>();
    	}
    	try {
    		pm.getSession().getLock().lock();
    		@SuppressWarnings("unchecked")
	    	SynchronizedROIteratorC<T> it = new SynchronizedROIteratorC<T>(
	    			(CloseableIterator<T>) pm.getSession().loadAllInstances(
	    		        extClass, subclasses, !ignoreCache), pm.getSession().getLock());
	    	allIterators.add(it);
	    	return it;
    	} finally {
    		pm.getSession().getLock().unlock();
    	}
    }

    /**
     * @see Extent#close(java.util.Iterator)
     */
    @Override
	public void close(Iterator<T> i) {
    	SynchronizedROIteratorC.class.cast(i).close();
        allIterators.remove(i);
    }

    /**
     * @see Extent#closeAll()
     */
    @Override
	public void closeAll() {
        for (SynchronizedROIteratorC<T> i: allIterators) {
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
	public Class<T> getCandidateClass() {
		return extClass;
	}

	@Override
	public FetchPlan getFetchPlan() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
}
