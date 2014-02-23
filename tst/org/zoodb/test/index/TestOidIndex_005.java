/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.zoodb.jdo.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.StorageRootInMemory;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.util.CloseableIterator;

/**
 * Test harness for a rare problem with the FSM when removing and adding pages out of order.
 * 
 * The problem was that after a leaf-page-split with resulting inner page split, the new pages was 
 * added to the wrong higher inner pages if there where other pages with higher values in the index.
 * 
 * @author Tilmann Zaeschke
 */
public class TestOidIndex_005 {

	private static final int[] I = { 
		1,13,0,1,14,0,1,18,0,1,20,0,1,21,0,-1,13,-1,14,-1,18,
		-1,20,-1,21,1,30,0,1,35,0,1,36,0,1,40,0,1,39,0,-1,30,
		-1,35,-1,36,-1,39,-1,40,1,29,0,1,13,0,1,31,0,1,32,0,
		1,33,0,1,34,0,1,14,0,1,18,0,1,20,0,1,21,0,1,45,0,1,44,0,
		-1,13,-1,14,-1,18,-1,20,-1,21,-1,29,-1,31,1,51,0,1,52,0,
		1,53,0,1,65,0,1,66,0,1,32,-1,1,33,-1,1,69,0,1,70,0,-1,32,
		-1,33,-1,34,-1,44,-1,45,-1,51,-1,52,-1,53,-1,65,1,14,0,
//		1,18,0,1,64,0,1,20,0,1,21,0,1,66,-1,1,69,-1,1,32,0,1,33,0,
//		-1,66,-1,69,-1,14,-1,18,-1,20,-1,21,-1,32,-1,33,-1,64,
//		-1,70,1,30,0,1,35,0,1,36,0,1,39,0,1,40,0,1,47,0,1,48,0,
//		1,49,0,1,13,0,1,34,0,1,44,0,1,54,0,1,55,0,1,56,0,1,57,0,
//		1,58,0,1,59,0,1,60,0,1,61,0,1,62,0,1,63,0,1,45,0,1,51,0,
//		1,52,0,1,69,0,1,66,0,-1,13,-1,30,-1,34,-1,35,-1,36,-1,39,
//		-1,40,-1,44,-1,45,-1,47,-1,48,-1,49,-1,51, 
		};
	
	@Test
	public void testIndexUnique() {
		StorageChannel paf = new StorageRootInMemory(64);
		PagedUniqueLongLong ind = new PagedUniqueLongLong(DATA_TYPE.GENERIC_INDEX, paf);

		Map<Long, Long> map = new HashMap<Long, Long>(); 
		
		//build index
		for (int i = 0; i < I.length; i++) {
			long x = I[i];
			if (x==-1) {
				//remove
				i++;
				long k = I[i];
				ind.removeLong(k);
				if (!map.containsKey(k)) {
					fail("i=" + i + " k=" + k);
				}
				map.remove(k);
				
			} else if (x==1) {
				//add
				i++;
				long k = I[i];
				i++;
				long v = I[i];
				ind.insertLong(k, v);
				if (map.containsKey(k)) {
					//v=-1 are invalidated pages in FSM
					if (v!=-1) {
						fail("i=" + i + " k=" + k + " v=" + v);
					}
				}
				map.put(k, v);
			} else {
				throw new IllegalStateException("i=" + x);
			}
		}

		
		//now compare elements
		CloseableIterator<LLEntry> indIter = ind.iterator(1, Long.MAX_VALUE);
		int n = 0;
		while (indIter.hasNext()) {
			n++;
			LLEntry e = indIter.next();
			assertTrue(map.containsKey(e.getKey()));
			assertEquals((long)map.get(e.getKey()), e.getValue());
		}
		
		assertEquals(map.size(), n);
	}
}
