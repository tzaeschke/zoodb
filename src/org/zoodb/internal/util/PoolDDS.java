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
package org.zoodb.internal.util;

import java.util.concurrent.locks.ReentrantLock;

import org.zoodb.internal.DataDeSerializer;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.ObjectReader;

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
    private final IOResourceProvider file;
	
    public PoolDDS(IOResourceProvider file, AbstractCache cache) {
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
