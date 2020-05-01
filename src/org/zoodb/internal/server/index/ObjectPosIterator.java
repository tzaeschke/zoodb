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

import org.zoodb.api.impl.ZooPC;
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
public class ObjectPosIterator implements CloseableIterator<ZooPC> {

	private final PagedPosIndex.ObjectPosIteratorMerger iter;
	private final boolean skipIfCached;
	private final DataDeSerializer dds;
	private ZooPC pc = null;
	
	public ObjectPosIterator(PagedPosIndex.ObjectPosIteratorMerger iter, AbstractCache cache, 
	        ObjectReader raf, boolean skipIfCached) {
		this.iter = iter;
        this.dds = new DataDeSerializer(raf, cache);
        this.skipIfCached = skipIfCached;
        findNext();
	}

	@Override
	public boolean hasNext() {
	    return pc != null;
	}

	@Override
	public ZooPC next() {
	    ZooPC pc2 = pc;
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
