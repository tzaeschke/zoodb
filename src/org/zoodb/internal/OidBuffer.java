/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	 * @param oid OID to check
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

	public static boolean isValid(long oid) {
		if (oid > 0) {
			return true;
		}
		if (oid == Session.OID_NOT_ASSIGNED) {
			return false;
		}
		throw DBLogger.newUser("Invalid OID: " + oid);
	}

}
