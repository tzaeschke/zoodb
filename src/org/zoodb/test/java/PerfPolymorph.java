package org.zoodb.test.java;

import org.junit.Test;

public class PerfPolymorph {

	private static final long MAX = 10000000;
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
	
	private static class Cls1 implements I1, I2, I3 {
		public int getX() {
			return 1;
		}
	}
	
	private static class Cls2 implements I2, I3 {
		public int getX() {
			return 1;
		}
	}
	
	private static class Cls3 implements I3 {
		public int getX() {
			return 1;
		}
	}
	
	@Test
	public void testPolyMorphArry() {
		int N = (int) MAX;
		Cls1 o1 = new Cls1();
		Cls2 o2 = new Cls2();
		Cls3 o3 = new Cls3();
		Cls1[] a0 = new Cls1[N];
		I1[] a1 = new I1[N];
		I2[] a2 = new I2[N];
		I3[] a3 = new I3[N];
		I2[] xx = {o1, o2};
		I3[] xxx = {o1, o2, o3};
		
		for (int i = 0; i < N; i++) {
			a0[i] = o1;
			a1[i] = o1;
			a2[i] = xx[i%2];
			a3[i] = xxx[i%3];
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
