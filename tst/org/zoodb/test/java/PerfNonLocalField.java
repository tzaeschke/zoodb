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


/**
 * Compares performance of local versus non-local fields.
 * 
 * Result: It doesn't matter at all! If anything, then fields of the non-inner class are faster.
 * 
 * @author Tilmann Zaeschke
 */
public class PerfNonLocalField {

	private static final int N = 10000;
	private long timer;

	
	private boolean bnl;
	private long lnl;
	
	
	
	private class TestMe {
		private final int N = PerfNonLocalField.N;
		private long ll;
		TestMe(long l, boolean b) {
			this.ll = l;
		}
		long getMethodLocalL() {
			final long lml = ll;
			long r = 0;
			for (int i = 0; i < N; i++) {
				r += lml;
				r++;
				r -= lml;
			}
			return r;
		}
		long getLocalL() {
			long r = 0;
			for (int i = 0; i < N; i++) {
				r += ll;
				r++;
				r -= ll;
			}
			return r;
		}
		long getNonLocalL() {
			long r = 0;
			for (int i = 0; i < N; i++) {
				r += lnl;
				r++;
				r -= lnl;
			}
			return r;
		}
	}
	

	
	
	public static void main(String[] args) {
		new PerfNonLocalField().run();
	}

	public void run() {
		TestMe tm = new TestMe(lnl, bnl);
		
		long n = 0;
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n += tm.getLocalL();
			}
			for (int j = 0; j < N; j++) {
				n += tm.getNonLocalL();
			}
			for (int j = 0; j < N; j++) {
				n += tm.getMethodLocalL();
			}
		}

		start();
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n += tm.getLocalL();
			}
		}
		stop("local long");
		
		start();
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n += tm.getNonLocalL();
			}
		}
		stop("non-local long");
		
		start();
		for (int i = 0; i < 100; i++) {
			for (int j = 0; j < N; j++) {
				n += tm.getMethodLocalL();
			}
		}
		stop("method loca long");
		
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
