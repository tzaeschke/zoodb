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
