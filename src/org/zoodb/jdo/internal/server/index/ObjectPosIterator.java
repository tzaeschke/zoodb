package org.zoodb.jdo.internal.server.index;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.PagedObjectAccess;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.CloseableIterator;

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
	private final boolean loadFromCache;
	private final DataDeSerializer dds;
	private PersistenceCapableImpl pc = null;
	
	public ObjectPosIterator(PagedPosIndex.ObjectPosIterator iter, AbstractCache cache, 
	        PagedObjectAccess raf, Node node, boolean loadFromCache) {
		this.iter = iter;
        this.dds = new DataDeSerializer(raf, cache, node, loadFromCache);
        this.loadFromCache = loadFromCache;
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
    		LLEntry oie = iter.nextOPI();
            pc = dds.readObject(oie.getKey());
    		if (loadFromCache) {
    		    if (!pc.jdoIsDeleted()) {
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
