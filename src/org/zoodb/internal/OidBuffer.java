/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal;

import org.zoodb.internal.util.DBLogger;

/**
 * 
 * @author ztilmann
 *
 */
public abstract class OidBuffer {

	public static final long NULL_REF = 0;

	private int allocSize = 100;
	
	private long[] oids;
	private int nextValidOidPos = -1;
	
	public final long allocateOid() {
		if (nextValidOidPos < 0) {
			oids = allocateMoreOids();
			nextValidOidPos = 0;
		}
		
		long oid = oids[nextValidOidPos];
		
		nextValidOidPos++;
		if (nextValidOidPos >= oids.length) {
			nextValidOidPos = -1;
			oids = null;
		}
		
		return oid;
	}
	
	public abstract long[] allocateMoreOids();
	
	public final void setOidAllocSize(int i) {
		if (i < 1 || i > 65535) { 
			throw DBLogger.newUser("Invalid OidAlloc size: " + i);
		}
		allocSize = i;
	}
	
	
	public final int getOidAllocSize() {
		return allocSize;
	}

	/**
	 * This needs to be called when users provide their own OIDs. The OID buffer needs to ensure
	 * that it will never return an OID that has been previously used by a user.
	 * @param oid
	 */
	public void ensureValidity(long oid) {
		if (oids == null) {
			return;
		}
		if (oid >= oids[oids.length-1]) {
			nextValidOidPos = -1;
			return;
		}
		while (oid >= oids[nextValidOidPos]) {
			nextValidOidPos++;
		}
	}

}
