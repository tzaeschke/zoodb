package org.zoodb.jdo.internal;

import javax.jdo.JDOUserException;

public abstract class OidBuffer {

	private int _allocSize = 100;
	
	private long[] _oids;
	private int _nextValidOid = -1;
	
	public final long allocateOid() {
		if (_nextValidOid < 0) {
			_oids = allocateMoreOids();
			_nextValidOid = 0;
		}
		
		long oid = _oids[_nextValidOid];
		
		_nextValidOid++;
		if (_nextValidOid >= _oids.length) {
			_nextValidOid = -1;
			_oids = null;
		}
		
		return oid;
	}
	
	public abstract long[] allocateMoreOids();
	
	public final void setOidAllocSize(int i) {
		if (i < 1 || i > 65535) { 
			throw new JDOUserException("Invalid OidAlloc size: " + i);
		}
		_allocSize = i;
	}
	
	
	public final int getOidAllocSize() {
		return _allocSize;
	}

}
