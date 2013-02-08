package org.zoodb.jdo.internal.server.index;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.GenericObject;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooHandle;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.ObjectReader;
import org.zoodb.jdo.internal.server.index.PagedPosIndex.ObjectPosIterator;
import org.zoodb.jdo.internal.util.CloseableIterator;

public class ZooHandleIteratorAdapter implements CloseableIterator<ZooHandle> {

    private final ZooClassDef def;
    private final ObjectPosIterator it;
    private final DataDeSerializer dds;
    
    public ZooHandleIteratorAdapter(ObjectPosIterator objectPosIterator, ZooClassDef def,
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
    public ZooHandle next() {
        long pos = it.nextPos();
        GenericObject go = new GenericObject(def, -1);
        dds.readGenericObject(go, BitTools.getPage(pos), BitTools.getOffs(pos));
        ZooHandle zh = new ZooHandle(go, def.jdoZooGetNode(), def.jdoZooGetContext().getSession(), 
                def.getVersionProxy());
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
