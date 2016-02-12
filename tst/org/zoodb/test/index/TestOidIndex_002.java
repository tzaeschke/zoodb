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
package org.zoodb.test.index;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.tools.ZooConfig;


/**
 * Test harness for a rare problem when adding OIDs out of order to the OID index during commit.
 * The problem occurred when adding over ~25000 object to a database. The OIDs are taken from the
 * cache (HashMap/Set), which is probably the reason why they are out of order.
 * 
 * The adding causes at some point the splitting of a leaf which causes incorrect creation of  a
 * new inner page. The page that contains the particular key (21128) still exists, but cannot be 
 * found anymore. 
 * 
 * This occurred with PageSize = 1024.
 * 
 * The problem occurred when a new leaf-page was added such that its root-inner-page had an 
 * overflow. The overflow was handled correctly and a new inner page was created. However,
 * the algorithm added the new leaf-page always to the 2nd (new) inner page, instead of adding it
 * to the 1st page, if applicable.
 * 
 * @author Tilmann Zaeschke
 *
 */
public class TestOidIndex_002 {

    
    @Test
    public void testIndex() {
    	StorageChannel paf = new StorageRootInMemory(ZooConfig.getFilePageSize());

    	PagedOidIndex ind = new PagedOidIndex(paf);
        boolean wasAdded = false;
       
        long[] I = loadData();
       
        for (int i = 0; i < I.length; i++) {
			long x = I[i];
			long oid = x;
//			if (x==-1) {
//				//remove
//				i++;
//				long k = I[i];
//				try {
//					ind.removeLong(k);
//				} catch (Exception e) {
//					System.out.println("key in map: " + map.containsKey(k));
//					System.out.println("R i=" + i + "   k=" + k);
//					throw new RuntimeException(e);
//				}
//				if (!map.containsKey(k)) {
//					fail("i=" + i + " k=" + k);
//				}
//				map.remove(k);
//			} else 
			if (x==1) {
				//add
				i++;
				long k = I[i];
				i++;
				int v = (int) I[i];
				try {
					ind.insertLong(k, v, 32 + i);
				} catch (Exception e) {
					System.out.println("I i=" + i + "   k=" + k + "/" + v);
					throw new RuntimeException(e);
				}
	            if (oid == 21128) {
	            	wasAdded = true;
	            }
	            if (wasAdded) {
	                assertNotNull( ind.findOid(21128) );
	            }
			} else {
				throw new IllegalStateException("i=" + x);
			}
        }
//        System.out.println("Index size: nInner=" + ind.statsGetInnerN() + "  nLeaf=" + 
//                ind.statsGetLeavesN());

        assertNotNull( ind.findOid(21128) );
    }
    
    
	private long[] loadData() {
		//return I;
		
		InputStream is = TestOidIndex_007_NoSuchElement.class.getResourceAsStream("index-002.log");
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
