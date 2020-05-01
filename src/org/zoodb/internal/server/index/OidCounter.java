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
		long expected;
		while (oid > (expected = lastAllocatedInMemory.get())) {
			lastAllocatedInMemory.compareAndSet(expected, oid);
		}
	}

	public long[] allocateOids(int oidAllocSize) {
		long l2 = lastAllocatedInMemory.addAndGet(oidAllocSize);
		long l1 = l2 - oidAllocSize;

		long[] ret = new long[(int) (l2-l1)];
		for (int i = 0; i < l2-l1; i++ ) {
			ret[i] = l1 + i + 1;
		}
		
		if (l2 < 0) {
			throw DBLogger.newFatalInternal("OID overflow after alloc: " + oidAllocSize +
					" / " + l2);
		}
		return ret;
	}

}
