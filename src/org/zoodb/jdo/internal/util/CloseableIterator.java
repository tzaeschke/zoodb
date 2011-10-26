package org.zoodb.jdo.internal.util;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T> {
	
	void close();

}
