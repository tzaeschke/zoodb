package org.zoodb.jdo.internal;

public class PropagationCorruptedException extends RuntimeException {

	/** serial ID */
	private static final long serialVersionUID = 1L;

	public PropagationCorruptedException(String string) {
		super(string);
	}

	public PropagationCorruptedException(String string, Throwable t) {
		super(string, t);
	}

}
