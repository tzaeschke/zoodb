package org.zoodb.test.server;

import static org.junit.Assert.*;
import org.junit.Test;
import org.zoodb.jdo.internal.server.index.BitTools;

public class TestBitTools {

	@Test
	public void testMinMax() {
//		String s0 = "";
		String s1 = "1";
		String s2 = "djfls7582943(*&*(a";
		
//		testMinMaxSub(s0);
		testMinMaxSub(s1);
		testMinMaxSub(s2);
	} 
	
	private void testMinMaxSub(String s) {
		long l0 = BitTools.toSortableLongMinHash(s);
		long l1 = BitTools.toSortableLong(s);
		long l2 = BitTools.toSortableLongMaxHash(s);
		assertTrue("" + l0 + " < " + l1, l0 < l1 );
		assertTrue("" + l2 + " > " + l1, l2 > l1 );
	}
	
	
	@Test
	public void testSorting() {
		String[] sa = { "1", "2", "3", "Msdfjdfa", "max", "maxmaxmax", "maxmaxmax@@@", "n" };
		for (int i = 1; i < sa.length; i++) {
			long l0 = BitTools.toSortableLong(sa[i-1]);
			long l1 = BitTools.toSortableLong(sa[i]);
			System.out.println(l0 + " / " + l1);
			assertTrue("" + sa[i-1] + " !< " + sa[i], l0 < l1 );
		}
	}
}
