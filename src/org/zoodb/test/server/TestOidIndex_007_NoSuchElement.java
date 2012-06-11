package org.zoodb.test.server;

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
import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.StorageInMemory;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.util.CloseableIterator;

/**
 * Check rare occurrences where elements in the PosINdex are visible to 
 * the iterator but not to the remove() method. 
 * 
 * @author Tilmann Zäschke
 */
public class TestOidIndex_007_NoSuchElement {

	@Test
	public void testIndexUnique() {
		FreeSpaceManager fsm = new FreeSpaceManager();
		StorageChannel paf = new StorageInMemory(64, fsm);
		fsm.initBackingIndexNew(paf);
		PagedUniqueLongLong ind = new PagedUniqueLongLong(paf);

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
