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
package org.zoodb.test.index2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.internal.util.CloseableIterator;

/**
 * Check rare occurrences where elements in the PosINdex are visible to 
 * the iterator but not to the remove() method. 
 * 
 * @author Tilmann Zaeschke
 */
public class TestOidIndex_008_LeafPageNotFound {

	@Test
	public void testIndexUnique() {
		StorageChannel paf = new StorageRootInMemory(48);
		PagedUniqueLongLong ind = new PagedUniqueLongLong(DATA_TYPE.GENERIC_INDEX, paf);

		Map<Long, Long> map = new HashMap<Long, Long>(); 
		long[] I = loadData();
		
		//build index
		for (int i = 0; i < I.length; i++) {
			long x = I[i];
			if (x==-1) {
				//remove
				i++;
				long k = I[i];
				try {
					ind.removeLong(k);
				} catch (Exception e) {
					System.out.println("key in map: " + map.containsKey(k));
					System.out.println("R i=" + i + "   k=" + k);
					throw new RuntimeException(e);
				}
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
				try {
					ind.insertLong(k, v);
				} catch (Exception e) {
					System.out.println("I i=" + i + "   k=" + k + "/" + v);
					throw new RuntimeException(e);
				}
				if (map.containsKey(k)) {
					//allow double adding for -1 (== invalidated pages in FSM)
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

	private long[] loadData() {
		//return I;
		
		InputStream is = TestOidIndex_008_LeafPageNotFound.class.getResourceAsStream("posIndex-007-075i.log");
		if (is==null) {
			throw new NullPointerException();
		}
		Scanner s = new Scanner(is);
		s.useDelimiter(",");
		ArrayList<Long> ret = new ArrayList<Long>();
		while (s.hasNext()) {
			ret.add(s.nextLong());
		}
		s.close();
		try {
			is.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		System.out.println("reading: " + ret.size());
		long[] ret2 = new long[ret.size()];
		for (int i = 0; i < ret.size(); i++) {
			ret2[i] = ret.get(i);
		}
		return ret2;
	}
}
