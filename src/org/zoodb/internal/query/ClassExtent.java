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
package org.zoodb.internal.query;

import java.util.ArrayList;
import java.util.Iterator;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.Session;
import org.zoodb.internal.SessionConfig;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.util.ClosableIteratorWrapper;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.SynchronizedROIterator;

/**
 * This class represents a class extent.
 * @param <T> The object type
 * 
 * @author Tilmann Zaeschke
 */
public class ClassExtent<T> implements Iterable<T> {
    
    private final ZooClassDef extClass;
    private final String className;
    private final boolean subclasses;
    private final ArrayList<SynchronizedROIterator<T>> allIterators = 
        new ArrayList<SynchronizedROIterator<T>>();
    private final Session pm;
    private final boolean ignoreCache;
    //This is used for aut-create schema mode, where a persistent class may not be in the database.
    private boolean isDummyExtent = false;
    private final SessionConfig sessionConfig;
    
    /**
     * @param pcClass The persistent class
     * @param subclasses Whether sub-classes should be returned
     * @param pm The PersistenceManager
     * @param ignoreCache Whether cached objects should be returned
     */
    public ClassExtent(Class<T> pcClass, 
            boolean subclasses, Session pm, boolean ignoreCache) {
        pm.checkActiveRead();
        this.className = pcClass.getName();
    	if (!ZooPC.class.isAssignableFrom(pcClass)) {
    		throw DBLogger.newUser("Class is not persistence capabale: " + 
    				pcClass.getName());
    	}
    	ZooClassDef def = pm.internalGetCache().getSchema(pcClass.getName());
    	if (pm.schema().getClass(pcClass) == null) {
    		if (pm.getConfig().getAutoCreateSchema()) {
    			isDummyExtent = true;
    		} else {
    			throw DBLogger.newUser("Class schema not defined: " + pcClass.getName());
    		}
    	}
        this.extClass = def;
        this.subclasses = subclasses;
        this.pm = pm;
        this.ignoreCache = ignoreCache;
        this.sessionConfig = pm.getConfig();
    }

    public ClassExtent(ZooClassDef def, Class<T> pcClass, 
            boolean subclasses, Session pm, boolean ignoreCache) {
        pm.checkActiveRead();
    	if (def == null) {
            this.className = pcClass.getName();
    		if (pm.getConfig().getAutoCreateSchema()) {
    			isDummyExtent = true;
    		} else {
    			throw DBLogger.newUser("Class schema not defined: " + className);
    		}
    	} else {
    		this.className = def.getClassName();
    	}
        this.extClass = def;
        this.subclasses = subclasses;
        this.pm = pm;
        this.ignoreCache = ignoreCache;
        this.sessionConfig = pm.getConfig();
    }

    /**
     * @see Iterable#iterator()
     */
    @Override
	public Iterator<T> iterator() {
		Session.LOGGER.info("extent.iterator() on class: {}", className);
    	if (isDummyExtent || 
    			(!pm.isActive() && 
    					!sessionConfig.getFailOnClosedQueries() &&
    					!sessionConfig.getNonTransactionalRead())) {
    		return new ClosableIteratorWrapper<>(sessionConfig.getFailOnClosedQueries());
    	}
    	try {
    		pm.getLock().lock();
    		@SuppressWarnings("unchecked")
	    	SynchronizedROIterator<T> it = new SynchronizedROIterator<T>(
	    			(CloseableIterator<T>) pm.loadAllInstances(
	    		        extClass, subclasses, !ignoreCache), pm.getLock());
	    	allIterators.add(it);
	    	return it;
    	} finally {
    		pm.getLock().unlock();
    	}
    }

	public void close(Iterator<T> i) {
    	CloseableIterator.class.cast(i).close();
        allIterators.remove(i);
    }

	public void closeAll() {
        for (SynchronizedROIterator<T> i: allIterators) {
            i.close();
        }
        allIterators.clear();
    }

	public boolean hasSubclasses() {
        return subclasses;
    }

	public Session getSession() {
        return pm;
    }
    
	public ZooClassDef getCandidateClass() {
		return extClass;
	}

}
