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
package org.zoodb.test.index;

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
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.internal.util.CloseableIterator;

/**
 * Check rare occurrences where elements in the PosINdex are visible to 
 * the iterator but not to the remove() method. 
 * 
 * @author Tilmann Zaeschke
 */
public class TestOidIndex_007_NoSuchElement {

	@Test
	public void testIndexUnique() {
		IOResourceProvider paf = new StorageRootInMemory(64).createChannel();
		PagedUniqueLongLong ind = new PagedUniqueLongLong(PAGE_TYPE.GENERIC_INDEX, paf);

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
		CloseableIterator<LongLongIndex.LLEntry> indIter = ind.iterator(1, Long.MAX_VALUE);
		int n = 0;
		while (indIter.hasNext()) {
			n++;
			LongLongIndex.LLEntry e = indIter.next();
			assertTrue(map.containsKey(e.getKey()));
			assertEquals((long)map.get(e.getKey()), e.getValue());
		}
		
		assertEquals(map.size(), n);
	}

	private long[] loadData() {
		//return I;
		
		InputStream is = TestOidIndex_007_NoSuchElement.class.getResourceAsStream("posIndex-007-075i.log");
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
		
		//System.out.println("reading: " + ret.size());
		long[] ret2 = new long[ret.size()];
		for (int i = 0; i < ret.size(); i++) {
			ret2[i] = ret.get(i);
		}
		return ret2;
	}
}
