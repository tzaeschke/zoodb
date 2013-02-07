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
package org.zoodb.jdo;

import java.util.ArrayList;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.internal.util.CloseableIterator;

/**
 * This class implements JDO behavior for the class Extent.
 * @param <T>
 * 
 * @author Tilmann Zäschke
 */
public class ExtentImpl<T> implements Extent<T> {
    
    private final Class<T> extClass;
    private final boolean subclasses;
    private final ArrayList<CloseableIterator<T>> allIterators = 
        new ArrayList<CloseableIterator<T>>();
    private final PersistenceManagerImpl pm;
    private final boolean ignoreCache;
    
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
    	if (ZooSchema.locateClass(pm, pcClass) == null) {
    	    throw new JDOUserException("Class schema not defined: " + pcClass.getName());
    	}
        this.extClass = pcClass;
        this.subclasses = subclasses;
        this.pm = pm;
        this.ignoreCache = ignoreCache;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#iterator()
     */
    @Override
	public Iterator<T> iterator() {
    	@SuppressWarnings("unchecked")
		CloseableIterator<T> it = (CloseableIterator<T>) pm.getSession().loadAllInstances(
    		        extClass, subclasses, !ignoreCache);
    	allIterators.add(it);
    	return it;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#close(java.util.Iterator)
     */
    @Override
	public void close(Iterator<T> i) {
        CloseableIterator.class.cast(i).close();
        allIterators.remove(i);
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#closeAll()
     */
    @Override
	public void closeAll() {
        for (CloseableIterator<T> i: allIterators) {
            i.close();
        }
        allIterators.clear();
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#hasSubclasses()
     */
    @Override
	public boolean hasSubclasses() {
        return subclasses;
    }

    /**
     * @see org.zoodb.jdo.oldStuff.Extent#getPersistenceManager()
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
