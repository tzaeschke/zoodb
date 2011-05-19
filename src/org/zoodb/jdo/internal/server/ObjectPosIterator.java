package org.zoodb.jdo.internal.server;

import java.lang.reflect.Field;
import java.util.Iterator;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.SerialInput;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.server.index.PagedOidIndex.FilePos;
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
public class ObjectPosIterator implements Iterator<PersistenceCapableImpl> {

	private final Iterator<FilePos> iter;  
	private final AbstractCache cache;
	private final PageAccessFile raf;
	private final Node node;
	private DataDeSerializer dds;
	
	public ObjectPosIterator(Iterator<FilePos> iter, AbstractCache cache, 
			PageAccessFile raf, Node node) {
		this.iter = iter;
		this.cache = cache;
		this.raf = raf;
		this.node = node;
//        dds = new DataDeSerializer(raf, cache, node);
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public PersistenceCapableImpl next() {
		FilePos oie = iter.next();
		raf.seekPage(oie.getPage(), oie.getOffs(), true);
        dds = new DataDeSerializer(raf, cache, node);
		return dds.readObject(oie.getOID());
	}

	@Override
	public void remove() {
		// do we need this? Should we allow it? I guess it fails anyway in the LLE-iterator.
		iter.remove();
	}
}
