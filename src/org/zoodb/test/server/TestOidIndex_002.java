package org.zoodb.test.server;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;

import org.junit.Test;
import org.zoodb.jdo.internal.Config;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.PageAccessFileInMemory;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;


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
 * @author Tilmann Zäschke
 *
 */
public class TestOidIndex_002 {

    
    @Test
    public void testIndex() {
    	FreeSpaceManager fsm = new FreeSpaceManager();
    	PageAccessFile paf = new PageAccessFileInMemory(Config.getFilePageSize(), fsm);
    	//fsm.initBackingIndexLoad(paf, 7, 8);
    	fsm.initBackingIndexNew(paf);

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
