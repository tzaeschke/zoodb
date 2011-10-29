package org.zoodb.jdo.internal;

import javax.jdo.JDOUserException;

public abstract class OidBuffer {

	public static final long NULL_REF = 0;

	private int allocSize = 100;
	
	private long[] oids;
	private int nextValidOid = -1;
	
	public final long allocateOid() {
		if (nextValidOid < 0) {
			oids = allocateMoreOids();
			nextValidOid = 0;
		}
		
		long oid = oids[nextValidOid];
		
		nextValidOid++;
		if (nextValidOid >= oids.length) {
			nextValidOid = -1;
			oids = null;
		}
		
		return oid;
	}
	
	public abstract long[] allocateMoreOids();
	
	public final void setOidAllocSize(int i) {
		if (i < 1 || i > 65535) { 
			throw new JDOUserException("Invalid OidAlloc size: " + i);
		}
		allocSize = i;
	}
	
	
	public final int getOidAllocSize() {
		return allocSize;
	}

}
