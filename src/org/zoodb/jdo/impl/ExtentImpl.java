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

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.FetchPlan;
import javax.jdo.PersistenceManager;

import org.zoodb.internal.query.ClassExtent;

/**
 * This class implements JDO behavior for the class Extent.
 * @param <T> The object type
 * 
 * @author Tilmann Zaeschke
 */
public class ExtentImpl<T> implements Extent<T> {

	private final ClassExtent<T> classExtent;

    /**
     * @param pcClass The persistent class
     * @param subclasses Whether sub-classes should be returned
     * @param pm The PersistenceManager
     * @param ignoreCache Whether cached objects should be returned
     */
    public ExtentImpl(Class<T> pcClass, 
            boolean subclasses, PersistenceManagerImpl pm, boolean ignoreCache) {
    	this.classExtent = new ClassExtent<>(pcClass, subclasses, pm.getSession(), ignoreCache);
    }

    /**
     * @see Extent#iterator()
     */
    @Override
	public Iterator<T> iterator() {
    	return classExtent.iterator();
    }

    /**
     * @see Extent#close(java.util.Iterator)
     */
    @Override
	public void close(Iterator<T> i) {
    	classExtent.close(i);
    }

    /**
     * @see Extent#closeAll()
     */
    @Override
	public void closeAll() {
        classExtent.closeAll();
    }

    /**
     * @see Extent#hasSubclasses()
     */
    @Override
	public boolean hasSubclasses() {
        return classExtent.hasSubclasses();
    }

    /**
     * @see Extent#getPersistenceManager()
     */
    @Override
	public PersistenceManager getPersistenceManager() {
        return (PersistenceManager)classExtent.getSession().getExternalSession();
    }
    
	@SuppressWarnings("unchecked")
	@Override
	public Class<T> getCandidateClass() {
		return (Class<T>) classExtent.getCandidateClass().getJavaClass();
	}

	@Override
	public FetchPlan getFetchPlan() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}
}
