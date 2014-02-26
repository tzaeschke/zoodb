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
package org.zoodb.internal.server.index;

import java.util.NoSuchElementException;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.DataDeSerializer;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.DiskAccessOneFile;
import org.zoodb.internal.server.ObjectReader;
import org.zoodb.internal.server.index.AbstractPagedIndex.AbstractPageIterator;
import org.zoodb.internal.server.index.PagedUniqueLongLong.LLEntry;
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
public class ObjectIterator implements CloseableIterator<ZooPCImpl> {

	private final LLIterator iter;  
	private final DiskAccessOneFile file;
	private final DataDeSerializer deSer;
	private final boolean loadFromCache;
	private final AbstractCache cache;
	private ZooPCImpl pc = null;
	
	/**
	 * Object iterator.
	 * 
	 * The last three fields can be null. If they are, the objects are simply returned and no checks
	 * are performed.
	 * 
	 * @param iter
	 * @param cache
	 * @param file
	 */
	public ObjectIterator(AbstractPageIterator<LLEntry> iter, AbstractCache cache, 
			DiskAccessOneFile file, ObjectReader in, boolean loadFromCache) {
		this.iter = (LLIterator) iter;
		this.file = file;
		this.deSer = new DataDeSerializer(in, cache);
		this.loadFromCache = loadFromCache; 
		this.cache = cache;
		findNext();
	}

    @Override
    public void refresh() {
        iter.refresh();
    }

	@Override
	public boolean hasNext() {
		return pc != null;
	}

	@Override
	public ZooPCImpl next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		ZooPCImpl ret = pc;
		findNext();
		return ret;
	}
	
	private void findNext() {
		while (iter.hasNextULL()) {
			LLEntry e = iter.nextULL();
			
			//try loading from cache first
			if (loadFromCache) {
	            long oid = e.getValue();
	            ZooPCImpl co = cache.findCoByOID(oid);
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
