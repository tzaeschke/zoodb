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
package org.zoodb.internal.server.index;

import java.util.NoSuchElementException;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.DataDeSerializer;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.DiskAccessOneFile;
import org.zoodb.internal.server.ObjectReader;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.util.CloseableIterator;

/**
 * TODO
 * This class can be improved in various ways:
 * a) Implement batch loading
 * b) Start a second thread that loads the next object after the previous one has been 
 *    delivered. 
 * c) Implement this iterator also in other reader classes.
 * 
 * @author Tilmann Zaeschke
 */
public class ObjectIterator implements CloseableIterator<ZooPC> {

	private final LLEntryIterator iter;  
	private final DiskAccessOneFile file;
	private final DataDeSerializer deSer;
	private final boolean loadFromCache;
	private final AbstractCache cache;
	private ZooPC pc = null;
	
	/**
	 * Object iterator.
	 * 
	 * The last three fields can be null. If they are, the objects are simply returned and no checks
	 * are performed.
	 * 
	 * @param iter An entry iterator
	 * @param cache The cache
	 * @param file File handle
	 * @param in Object input stream
	 * @param loadFromCache Whether to load from cache, if possible
	 */
	public ObjectIterator(LLEntryIterator iter, AbstractCache cache, 
			DiskAccessOneFile file, ObjectReader in, boolean loadFromCache) {
		this.iter = iter;
		this.file = file;
		this.deSer = new DataDeSerializer(in, cache);
		this.loadFromCache = loadFromCache; 
		this.cache = cache;
		findNext();
	}

	@Override
	public boolean hasNext() {
		return pc != null;
	}

	@Override
	public ZooPC next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		ZooPC ret = pc;
		findNext();
		return ret;
	}
	
	private void findNext() {
		while (iter.hasNextULL()) {
			LongLongIndex.LLEntry e = iter.nextULL();
			
			//try loading from cache first
			if (loadFromCache) {
	            long oid = e.getValue();
	            ZooPC co = cache.findCoByOID(oid);
	            if (co != null && !co.jdoZooIsStateHollow()) {
	                if (co.jdoZooIsDeleted()) {
	                    continue;
	                }
	                this.pc = co;
	                return;
	                //TODO we also need to add objects that meet a query only because of local 
	                //updates to the object
	            }
	            //TODO small optimization: if this returns null, forward this to deserializer
	            //telling that cache-lok-up is pointless.
			}
			
			this.pc = file.readObject(deSer, e.getValue());
			return;
		}
		close();
	}

	@Override
	public void remove() {
		// do we need this? Should we allow it? I guess it fails anyway in the LLE-iterator.
		iter.remove();
	}
	
	
	@Override
	public void close() {
		pc = null;
		iter.close();
	}
}
