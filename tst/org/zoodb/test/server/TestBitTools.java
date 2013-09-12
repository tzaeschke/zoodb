/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.zoodb.jdo.internal.server.index.BitTools;

public class TestBitTools {

	@Test
	public void testMinMax() {
		String s0 = "";
		String s1 = "1";
		String s2 = "djfls7582943(*&*(a";
		
		testMinMaxSubEq(s0);
		testMinMaxSub(s1);
		testMinMaxSub(s2);
	} 
	
	private void testMinMaxSubEq(String s) {
		long l0 = BitTools.toSortableLongMinHash(s);
		long l1 = BitTools.toSortableLong(s);
		long l2 = BitTools.toSortableLongMaxHash(s);
		assertTrue("" + l0 + " < " + l1, l0 <= l1 );
		assertTrue("" + l2 + " > " + l1, l2 >= l1 );
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

	@Test
	public void testSortingDouble() {
		//special case: -0.0 == 0.0
		long l0x = BitTools.toSortableLong(-0.0);
		long l1x = BitTools.toSortableLong(0.0);
		assertTrue(l0x == l1x );
		
		
		double[] sa = { -231.3, -12., -1.1, -0.0232, -0.0001, 0., 0.0001, 0.002, 1, 12, 1231 };
		for (int i = 1; i < sa.length; i++) {
			long l0 = BitTools.toSortableLong(sa[i-1]);
			long l1 = BitTools.toSortableLong(sa[i]);
			assertTrue("" + sa[i-1] + " !< " + sa[i] + " --- " + l0 + " !< " + l1, l0 < l1 );
		}
	}

	/**
	 * Test that sorting is still correct even if only the mantissa changes.
	 */
	@Test
	public void testSortingDoubleMantissa() {
		double[] sa = { -2.31121, -2.3112, -2.3111, 0.0001, 0.0002, 0.00021, 1231.1, 1231.11 };
		for (int i = 1; i < sa.length; i++) {
			long l0 = BitTools.toSortableLong(sa[i-1]);
			long l1 = BitTools.toSortableLong(sa[i]);
			assertTrue("" + sa[i-1] + " !< " + sa[i] + " --- " + l0 + " !< " + l1, l0 < l1 );
		}
	}
	
	@Test
	public void testSymmetryDouble() {
		double[] sa = { -231.3, -12., -1.1, -0.0232, -0.0001, 0., 0.0001, 0.002, 1, 12, 1231 };
		for (double d0: sa) {
			checkSymmetry(d0);
		}
		
		checkSymmetry(Double.MAX_VALUE);
		checkSymmetry(Double.MIN_VALUE);
		checkSymmetry(Double.MIN_NORMAL);
		checkSymmetry(Double.NaN);
		checkSymmetry(1./0.);
		checkSymmetry(-1./0.);
		checkSymmetry(-0.0);
		checkSymmetry(0.0);
	}
	
	private void checkSymmetry(double d) {
		long l = BitTools.toSortableLong(d);
		double d2 = BitTools.toDouble(l);
		if (Double.isNaN(d) && Double.isNaN(d2)) {
			return; //ok
		}
		assertTrue( d + " != " + d2 + "  l=" + l, d == d2 );
	}
	
	@Test
	public void testSortingFloat() {
		//special case: -0.0 == 0.0
		long l0x = BitTools.toSortableLong(-0.0);
		long l1x = BitTools.toSortableLong(0.0);
		assertTrue(l0x == l1x );
		
		
		float[] sa = { -231.3f, -12f, -1.1f, -0.0232f, -0.0001f, 0f, 0.0001f, 0.002f, 1, 12, 1231 };
		for (int i = 1; i < sa.length; i++) {
			long l0 = BitTools.toSortableLong(sa[i-1]);
			long l1 = BitTools.toSortableLong(sa[i]);
			assertTrue("" + sa[i-1] + " !< " + sa[i] + " --- " + l0 + " !< " + l1, l0 < l1 );
		}
	}
	
	@Test
	public void testSymmetryFloat() {
		float[] sa = { -231.3f, -12f, -1.1f, -0.0232f, -0.0001f, 0f, 0.0001f, 0.002f, 1, 12, 1231 };
		for (float d0: sa) {
			checkSymmetry(d0);
		}
		
		checkSymmetry(Float.MAX_VALUE);
		checkSymmetry(Float.MIN_VALUE);
		checkSymmetry(Float.MIN_NORMAL);
		checkSymmetry(Float.NaN);
		checkSymmetry(1.f/0.f);
		checkSymmetry(-1.f/0.f);
		checkSymmetry(-0.0f);
		checkSymmetry(0.0f);
	}
	
	/**
	 * Check whether the input value matches the output value, i.e. whether encoding and decoding
	 * a value results in the original value.
	 * @param d
	 */
	private void checkSymmetry(float d) {
		long l = BitTools.toSortableLong(d);
		float d2 = BitTools.toFloat(l);
		if (Float.isNaN(d) && Float.isNaN(d2)) {
			return; //ok
		}
		assertTrue( d + " != " + d2 + "  l=" + l, d == d2 );
	}
	
	@Test
	public void testRandom() {
		Random R = new Random(0);
		int N = 10000;
		double[] da = new double[N];
		long[] la = new long[da.length];
		for (int i = 0; i < da.length; i++) {
			da[i] = (R.nextDouble()*2-1)*Double.MAX_VALUE;
			la[i] = BitTools.toSortableLong(da[i]);
		}
		Arrays.sort(da);
		Arrays.sort(la);
		for (int i = 0; i < da.length; i++) {
			assertTrue(BitTools.toDouble(la[i])  + " / " + da[i], 
					Math.abs(BitTools.toDouble(la[i])-da[i]) < Double.MAX_VALUE);
		}
		
	}
}


