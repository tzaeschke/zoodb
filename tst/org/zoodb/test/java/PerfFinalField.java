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
