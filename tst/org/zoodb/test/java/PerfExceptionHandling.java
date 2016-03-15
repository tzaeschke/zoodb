/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
