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
