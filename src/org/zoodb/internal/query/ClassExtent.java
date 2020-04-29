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
    	ZooClassDef def = (ZooClassDef) pm.internalGetCache().getSchema(pcClass.getName());
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
