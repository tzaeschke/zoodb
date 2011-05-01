package org.zoodb.test.java;

import java.io.IOException;

/**
 * Results: With Java, all results are basically the same, around 0.203 with occasional peaks of
 * 0.219.
 * @author Tilmann Zäschke
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
