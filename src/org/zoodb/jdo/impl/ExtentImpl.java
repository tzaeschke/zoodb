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
package org.zoodb.jdo.impl;

import java.util.ArrayList;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.MergingIterator;

/**
 * This class implements JDO behavior for the class Extent.
 * @param <T>
 * 
 * @author Tilmann Zaeschke
 */
public class ExtentImpl<T> implements Extent<T> {
    
    private final Class<T> extClass;
    private final boolean subclasses;
    private final ArrayList<CloseableIterator<T>> allIterators = 
        new ArrayList<CloseableIterator<T>>();
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
    	if (!ZooPCImpl.class.isAssignableFrom(pcClass)) {
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
    	@SuppressWarnings("unchecked")
		CloseableIterator<T> it = (CloseableIterator<T>) pm.getSession().loadAllInstances(
    		        extClass, subclasses, !ignoreCache);
    	allIterators.add(it);
    	return it;
    }

    /**
     * @see Extent#close(java.util.Iterator)
     */
    @Override
	public void close(Iterator<T> i) {
        CloseableIterator.class.cast(i).close();
        allIterators.remove(i);
    }

    /**
     * @see Extent#closeAll()
     */
    @Override
	public void closeAll() {
        for (CloseableIterator<T> i: allIterators) {
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

    public void refresh() {
        for (CloseableIterator<T> i: allIterators) {
            i.refresh();
        }
    }
}
