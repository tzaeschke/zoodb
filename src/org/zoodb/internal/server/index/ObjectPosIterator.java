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

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.DataDeSerializer;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.ObjectReader;
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
public class ObjectPosIterator implements CloseableIterator<ZooPCImpl> {

	private final PagedPosIndex.ObjectPosIteratorMerger iter;
	private final boolean skipIfCached;
	private final DataDeSerializer dds;
	private ZooPCImpl pc = null;
	
	public ObjectPosIterator(PagedPosIndex.ObjectPosIteratorMerger iter, AbstractCache cache, 
	        ObjectReader raf, boolean skipIfCached) {
		this.iter = iter;
        this.dds = new DataDeSerializer(raf, cache);
        this.skipIfCached = skipIfCached;
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
	    ZooPCImpl pc2 = pc;
	    findNext();
	    return pc2;
	}
	
	private void findNext() {
	    while (iter.hasNextOPI()) {
	        long pos = iter.nextPos();
	        pc = dds.readObject(BitTools.getPage(pos), BitTools.getOffs(pos), skipIfCached);
	        if (skipIfCached) {
    		    if (!pc.jdoZooIsDeleted()) {
    		        return;
    		    }
    		} else {
    		    return;
    		}
	    }
	    pc = null;
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
