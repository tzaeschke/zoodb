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
package org.zoodb.test.java;


public class PerfPolymorph {

	//private static final long MAX = 10000000;
	private static final long MAX = 1000000;
	private long timer = 0;
	
	private static interface I1 {
		int getX();
	}
	
	private static interface I2 {
		int getX();
	}
	
	private static interface I3 {
		int getX();
	}
	
	private static interface I4 {
		int getX();
	}
	
	private static class Cls1 implements I1, I2, I3, I4 {
		public int getX() {
			return 1;
		}
	}
	
	private static class Cls2 implements I2, I3, I4 {
		public int getX() {
			return 1;
		}
	}
	
	private static class Cls3 implements I3, I4 {
		public int getX() {
			return 1;
		}
	}
	
	private static class Cls4 implements I4 {
		public int getX() {
			return 1;
		}
	}
	
	public static void main(String[] args) {
		new PerfPolymorph().testPolyMorphArry();
	}

	public void testPolyMorphArry() {
		int N = (int) MAX;
		Cls1 o11 = new Cls1(), o12 = new Cls1(), o13 = new Cls1(), o14 = new Cls1();
		Cls2 o21 = new Cls2(), o22 = new Cls2();
		Cls3 o3 = new Cls3();
		Cls4 o4 = new Cls4();
		
		Cls1[] a0 = new Cls1[N];
		I1[] a1 = new I1[N];
		I2[] a2 = new I2[N];
		I3[] a3 = new I3[N];
		I4[] a4 = new I4[N];
		//Always use four objects to reduce cache skew (less objects in cache)
		Cls1[] c = {o11, o12, o13, o14}; //o1
		I1[] x1 = {o11, o12, o13, o14};  //o1
		I2[] x2 = {o11, o21, o12, o22};  //o2, o2
		I3[] x3 = {o11, o21, o3, o12};   //o1, o2, o3
		I4[] x4 = {o11, o21, o3, o4};    //o1, o2, o3, o4
		
		for (int i = 0; i < N; i++) {
			int ii = i%4;
			a0[i] = c[ii];
			a1[i] = x1[ii];
			a2[i] = x2[ii];
			a3[i] = x3[ii];
			a4[i] = x4[ii];
		}
		
		long n = 0;
		for (int i = 0; i < 10; i++) {
			start();
			for (int k = 0; k < 10; k++)
			for (int j = 0; j < MAX; j++) {
				n+= a0[j].getX();
			}
			stop("Class");

			start();
			for (int k = 0; k < 10; k++)
			for (int j = 0; j < MAX; j++) {
				n+= a1[j].getX();
			}
			stop("IF 1 impl");

			start();
			for (int k = 0; k < 10; k++)
			for (int j = 0; j < MAX; j++) {
				n+= a2[j].getX();
			}
			stop("IF 2 impl");

			start();
			for (int k = 0; k < 10; k++)
			for (int j = 0; j < MAX; j++) {
				n+= a3[j].getX();
			}
			stop("IF 3 impl");

			start();
			for (int k = 0; k < 10; k++)
			for (int j = 0; j < MAX; j++) {
				n+= a4[j].getX();
			}
			stop("IF 4 impl");
		}
		System.out.println("n=" + n);
	}
	
	private void start() {
		timer = System.currentTimeMillis();
	}
	
	private void stop(String str) {
		long t = System.currentTimeMillis() - timer;
		System.out.println(str + ": " + t);
	}
}
