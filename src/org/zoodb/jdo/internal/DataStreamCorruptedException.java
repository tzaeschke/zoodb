package org.zoodb.jdo.internal;

public class DataStreamCorruptedException extends RuntimeException {

	/** serial ID */
	private static final long serialVersionUID = 1L;

	public DataStreamCorruptedException(String string) {
		super(string);
	}

	public DataStreamCorruptedException(String string, Throwable t) {
		super(string, t);
	}

}
