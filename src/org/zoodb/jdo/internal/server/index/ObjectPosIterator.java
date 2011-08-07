package org.zoodb.jdo.internal.server.index;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.PageAccessFile;
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
	private final DataDeSerializer dds;
	
	public ObjectPosIterator(PagedPosIndex.ObjectPosIterator iter, AbstractCache cache, 
			PageAccessFile raf, Node node) {
		this.iter = iter;
        dds = new DataDeSerializer(raf, cache, node);
	}

	@Override
	public boolean hasNext() {
		return iter.hasNextOPI();
	}

	@Override
	public PersistenceCapableImpl next() {
		LLEntry oie = iter.nextOPI();
		return dds.readObject(oie.getKey());
	}

	@Override
	public void remove() {
		// do we need this? Should we allow it? I guess it fails anyway in the LLE-iterator.
		iter.remove();
	}
	
	@Override
	public void close() {
		iter.close();
	}
}
