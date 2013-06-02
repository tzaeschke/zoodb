/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
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
