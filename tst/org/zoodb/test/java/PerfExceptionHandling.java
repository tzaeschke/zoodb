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

import java.io.IOException;

/**
 * Results: With Java, all results are basically the same, around 0.203 with occasional peaks of
 * 0.219.
 * @author Tilmann Zaeschke
 *
 */
public class PerfExceptionHandling {
	public static void main(String[] args) throws IOException {
		new PerfExceptionHandling().run();
	}

	private void run() throws IOException {
		for (int i = 0; i < 10; i++) {
			runInner();
		}
		System.out.println("n=" + n);
	}
	
	private void runInner() throws IOException {
		noEx();
		noCheckedEx();
		tryInLoop();
		tryNotInLoop();
		noTry();
	}
	
	
	private static final int MAX = 100000000;
	private static long n = 0;
	
	private void noEx() {
		start("noEx");
		for (int i = 0; i < MAX; i++) {
			n = i;
			if (i == -1) {
				i++;
			}
		}
		stop("noEx");
	}
	
	private void noCheckedEx() {
		start("noCheckedEx");
		for (int i = 0; i < MAX; i++) {
			n = i;
			if (i == -1) {
				i++;
				throw new RuntimeException();
			}
		}
		stop("noCheckedEx");
	}
	
	private void tryInLoop() {
		start("tryInLoop");
		for (int i = 0; i < MAX; i++) {
			try {
				n = i;
				if (i == -1) {
					i++;
					throw new IOException();
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		stop("tryInLoop");
	}
	
	private void tryNotInLoop() {
		start("tryNotInLoop");
		try {
			for (int i = 0; i < MAX; i++) {
				n = i;
				if (i == -1) {
					i++;
					throw new IOException();
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		stop("tryNotInLoop");
	}
	
	private void noTry() throws IOException {
		start("noTry");
		for (int i = 0; i < MAX; i++) {
			n = i;
			if (i == -1) {
				i++;
				throw new IOException();
			}
		}
		stop("noTry");
	}
	
	private long _time;
	private void start(String msg) {
		_time = System.currentTimeMillis();
	}
	private void stop(String msg) {
		long t = System.currentTimeMillis() - _time;
		double td = t/1000.0;
		System.out.println(msg + ": " + td);
	}
}
