package org.zoodb.jdo.internal.server.index;

import java.util.Iterator;

public interface CloseableIterator<E> extends Iterator<E> {

	public void close();
	
}
