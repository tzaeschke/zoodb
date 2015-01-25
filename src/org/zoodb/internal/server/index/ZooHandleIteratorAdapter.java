/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

import org.zoodb.internal.DataDeSerializer;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.ZooHandleImpl;
import org.zoodb.internal.client.AbstractCache;
import org.zoodb.internal.server.ObjectReader;
import org.zoodb.internal.server.index.PagedPosIndex.ObjectPosIteratorMerger;
import org.zoodb.internal.util.CloseableIterator;

/**
 * 
 * @author ztilmann
 *
 */
public class ZooHandleIteratorAdapter implements CloseableIterator<ZooHandleImpl> {

    private final ObjectPosIteratorMerger it;
    private final DataDeSerializer dds;
    
    public ZooHandleIteratorAdapter(ObjectPosIteratorMerger objectPosIterator,
            ObjectReader in, AbstractCache cache) {
        this.dds = new DataDeSerializer(in, cache);
        this.it = objectPosIterator;
    }

    @Override
    public boolean hasNext() {
        return it.hasNextOPI();
    }

    @Override
    public ZooHandleImpl next() {
        long pos = it.nextPos();
        GenericObject go = dds.readGenericObject(BitTools.getPage(pos), BitTools.getOffs(pos));
        return go.getOrCreateHandle();
    }

    @Override
    public void remove() {
        it.remove();
    }

    @Override
    public void close() {
        it.close();
    }
}
