/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.internal.util;

import java.util.concurrent.locks.ReentrantLock;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.ObjectReader;
import org.zoodb.jdo.internal.server.StorageChannel;

/**
 * Pool for DataDeserializers.
 * 
 * @author ztilmann
 *
 */
public class PoolDDS {
    /** Main lock for all access */
    private final ReentrantLock lock = new ReentrantLock(false);
    private static final boolean CONCURRENT = false;
    private final DataDeSerializer[] items = new DataDeSerializer[10]; 
    private int count = 0;
    
    private final AbstractCache cache;
    private final StorageChannel file;
	
    public PoolDDS(StorageChannel file, AbstractCache cache) {
    	this.file = file;
    	this.cache = cache;
    }
    
	/**
     * 
     * @return An object from the pool or a new object if the pool is empty.
     */
    public final DataDeSerializer get() {
        lock();
        try {
            if (count == 0) {
            	ObjectReader poa = new ObjectReader(file);
                return new DataDeSerializer(poa, cache);
            }
            //TODO set to null?
            return items[--count];
        } finally {
            unlock();
        }
    }
    
    /**
     * Return an object. The object may be discarded if the pool is full.
     * @param e object to return
     */
    public final void offer(DataDeSerializer e) {
        //discard if pool is full
        lock();
        try {
            if (count < items.length) {
                items[count++] = e;
            }
        } finally {
            unlock();
        }
    }

    private final void lock() {
        if (CONCURRENT) {
            lock.lock();
        }
    }

    private final void unlock() {
        if (CONCURRENT) {
            lock.unlock();
        }
    }
}
