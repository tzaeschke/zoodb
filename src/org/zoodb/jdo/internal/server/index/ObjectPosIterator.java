package org.zoodb.jdo.internal.server.index;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.PagedObjectAccess;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * TODO
 * This class can be improved in various ways:
 * a) Implement batch loading
 * b) Start a second thread that loads the next object after the previous one has been 
 *    delivered. 
 * c) Implement this iterator also in other reader classes.
 * 
 * @author Tilmann Zäschke
 */
public class ObjectPosIterator implements CloseableIterator<PersistenceCapableImpl> {

	private final PagedPosIndex.ObjectPosIterator iter;
	private final boolean skipIfCached;
	private final DataDeSerializer dds;
	private PersistenceCapableImpl pc = null;
	
	public ObjectPosIterator(PagedPosIndex.ObjectPosIterator iter, AbstractCache cache, 
	        PagedObjectAccess raf, Node node, boolean skipIfCached) {
		this.iter = iter;
        this.dds = new DataDeSerializer(raf, cache, node);
        this.skipIfCached = skipIfCached;
        findNext();
	}

	@Override
	public boolean hasNext() {
	    return pc != null;
	}

	@Override
	public PersistenceCapableImpl next() {
	    PersistenceCapableImpl pc2 = pc;
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
