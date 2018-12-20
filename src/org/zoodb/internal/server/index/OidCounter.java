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
package org.zoodb.internal.server.index;

import java.util.concurrent.atomic.AtomicLong;

import org.zoodb.internal.util.DBLogger;

public class OidCounter {

	static final long MIN_OID = 100;

	private final AtomicLong lastAllocatedInMemory = new AtomicLong(MIN_OID);

	public long getLast() {
		return lastAllocatedInMemory.get();
	}

	public void update(long oid) {
		lastAllocatedInMemory.getAndAccumulate(oid, Math::max);
	}

	public long[] allocateOids(int oidAllocSize) {
		long l2 = lastAllocatedInMemory.addAndGet(oidAllocSize);
		long l1 = l2 - oidAllocSize;

		long[] ret = new long[oidAllocSize];
		for (int i = 0; i < ret.length; i++ ) {
			ret[i] = l1 + i + 1;
		}
		
		if (l2 < 0) {
			throw DBLogger.newFatalInternal("OID overflow after alloc: " + oidAllocSize +
					" / " + l2);
		}
		return ret;
	}

}
