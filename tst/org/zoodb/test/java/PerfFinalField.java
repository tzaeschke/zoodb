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
package org.zoodb.test.java;


public class PerfFinalField {

	private long timer;
	
	private static class TestMe {
		private final int x;
		private int y;
		TestMe(int x) {
			this.x = x;
		}
		void setY(int y) {
			this.y = y;
		}
//		int getX() {
//			return x;
//		}
//		int getY() {
//			return y;
//		}
	}
	
	private static final int N = 100000;

	private static class TestThread extends Thread {
		
		private TestMe[] a;
		
		@Override
		public void run() {
			try {
				sleep(100);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			for (int i = 0; i < N; i++) {
				a[i] = new TestMe(i);
				a[i].setY(i);
			}
		}
	}
	
	
	public static void main(String[] args) {
		new PerfFinalField().run();
	}

	public void run() {
		TestMe[] a = new TestMe[N];
//		for (int i = 0; i < N; i++) {
//			a[i] = new TestMe(i);
//			a[i].setY(i);
//		}
		TestThread tt = new TestThread();
		tt.a = a;
		tt.start();
		
//		try {
//			tt.join();
//		} catch (InterruptedException e) {
//			throw new RuntimeException(e);
//		}
		
		long n = 0;
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n += a[j].x;
			}
			for (int j = 0; j < N; j++) {
				n += a[j].y;
			}
		}

		start();
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n += a[j].x;
			}
		}
		stop("final X");
		
		start();
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n += a[j].y;
			}
		}
		stop("y");
		
		try {
			tt.join();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		
		System.out.println(n);
	}
	
	
	private void start() {
		timer = System.currentTimeMillis();
	}
	
	private void stop(String str) {
		long t = System.currentTimeMillis() - timer;
		System.out.println(str + ": " + t);
	}
}
