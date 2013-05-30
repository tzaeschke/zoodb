/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal.server.index;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.GenericObject;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooHandleImpl;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.ObjectReader;
import org.zoodb.jdo.internal.server.index.PagedPosIndex.ObjectPosIteratorMerger;
import org.zoodb.jdo.internal.util.CloseableIterator;

/**
 * 
 * @author ztilmann
 *
 */
public class ZooHandleIteratorAdapter implements CloseableIterator<ZooHandleImpl> {

    private final ZooClassDef def;
    private final ObjectPosIteratorMerger it;
    private final DataDeSerializer dds;
    
    public ZooHandleIteratorAdapter(ObjectPosIteratorMerger objectPosIterator, ZooClassDef def,
            ObjectReader in, AbstractCache cache, Node node) {
        this.dds = new DataDeSerializer(in, cache, node);
        this.it = objectPosIterator;
        this.def = def;
    }

    @Override
    public boolean hasNext() {
        return it.hasNextOPI();
    }

    @Override
    public ZooHandleImpl next() {
        long pos = it.nextPos();
        GenericObject go = new GenericObject(def, -1);
        dds.readGenericObject(go, BitTools.getPage(pos), BitTools.getOffs(pos));
        ZooHandleImpl zh = new ZooHandleImpl(go, def.getVersionProxy());
        return zh;
    }

    @Override
    public void remove() {
        it.remove();
    }

    @Override
    public void close() {
        it.close();
    }

    @Override
    public void refresh() {
        it.refresh();
    }

}
