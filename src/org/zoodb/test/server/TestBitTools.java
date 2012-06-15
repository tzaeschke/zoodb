/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
			assertTrue("" + sa[i-1] + " !< " + sa[i], l0 < l1 );
		}
	}
}
