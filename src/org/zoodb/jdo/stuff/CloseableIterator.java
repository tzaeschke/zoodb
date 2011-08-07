package org.zoodb.jdo.stuff;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T> {
	
	void close();

}
