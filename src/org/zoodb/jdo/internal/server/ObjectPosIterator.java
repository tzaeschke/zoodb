package org.zoodb.jdo.internal.server;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.index.BitTools;
import org.zoodb.jdo.internal.server.index.CloseableIterator;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
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

	private final CloseableIterator<LLEntry> iter;  
	private final PageAccessFile raf;
	private final DataDeSerializer dds;
	
	public ObjectPosIterator(CloseableIterator<LLEntry> iter, AbstractCache cache, 
			PageAccessFile raf, Node node) {
		this.iter = iter;
		this.raf = raf;
        dds = new DataDeSerializer(raf, cache, node);
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public PersistenceCapableImpl next() {
		LLEntry oie = iter.next();
		raf.seekPos(oie.getKey(), true);
		return dds.readObject();
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
	
	@Override
	protected void finalize() throws Throwable {
		iter.close();
		super.finalize();
	}
}
