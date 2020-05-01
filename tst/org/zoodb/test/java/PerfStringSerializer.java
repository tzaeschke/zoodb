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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

/**
 * Check performance of different ways to store a String into a ByteBuffer. 
 * 
 * 
 * @author Tilmann Zaeschke
 *
 */
public class PerfStringSerializer {
	
	private static final int MAX_J = 100;
	private static final int N = 10000;
	
//	private static final String S1 = "Hallo!\t\u1234";
	private static final String S2 = 
		"123456789 123456789 123456789 123456789 123456789 " +
		"123456789 123456789 123456789 123456789 123456789 " +
		"123456789 123456789 123456789 123456789 123456789 " +
		"123456789 123456789 123456789 123456789 123456789 " +
		"123456789 123456789 123456789 123456789 123456789 " +
		"123456789 123456789 123456789 123456789 123456789 " +
		"123456789 123456789 123456789 123456789 123456789 " +
		"123456789 123456789 123456789 123456789 123456789 ";
	
	private static final String[] S = {S2, S2, S2, S2}; 
	
//	private static final ByteBuffer buf = ByteBuffer.allocateDirect(N * 16);
	private static final ByteBuffer buf = ByteBuffer.allocateDirect(N * 800);
	
	public static void main(String[] args) {
		new PerfStringSerializer().run();
	}

	private void run() {
//		writeGetByte();
//		writeGetByte();
//		writeGetByte();

		writeToCharArray();
		writeToCharArray();
		writeToCharArray();
	}
	
	void writeGetByte() {
		start("write String.getBytes()");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < N; i++) {
		        byte[] ba = S[N%4].getBytes();
				buf.put(ba, 0, ba.length);
			}
			buf.rewind();
		}
		stop("write String.getBytes()");
	}
	
	void writeToCharArray() {
		start("write toCharArray");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < N; i++) {
				char[] ca = S[N%4].toCharArray();
				for (char c: ca)
					buf.putChar(c);
			}
			buf.rewind();
		}
		stop("write toCharArray");
		
		start("write byte[]");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < N; i++) {
				char[] ca = S[N%4].toCharArray();
				for (char c: ca) {
					buf.put( (byte) (c >> 8) ); //TODO >>> ?
					buf.put( (byte) (c) ); //TODO ? & 0x00FF
				}
			}
			buf.rewind();
		}
		stop("write byte[]");
		
		start("write putChar()");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < N; i++) {
				String s = S[N%4];
				for (int ii = 0; ii < s.length(); ii++) {
					buf.putChar(s.charAt(ii));
				}
			}
			buf.rewind();
		}
		stop("write putChar()");
		
		start("write CharBuffer/char[]");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < N; i++) {
				CharBuffer cb = buf.asCharBuffer();
				cb.put(S[N%4].toCharArray());
			}
			buf.rewind();
		}
		stop("write CharBuffer/char[]");
		start("write CharBuffer");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < N; i++) {
				CharBuffer cb = buf.asCharBuffer();
				String s = S[N%4];
				cb.put(s, 0, s.length());
				buf.position(buf.position()+2);
			}
			buf.rewind();
		}
		stop("write CharBuffer");
		start("write CharBuffer BB");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < N; i++) {
//				ByteBuffer bb = ByteBuffer.allocate(S.length() * 2);
//				CharBuffer cb = buf.asCharBuffer();
//				cb.put(S);
//				CharBuffer cb = CharBuffer.wrap(S.toCharArray());
//				cb.
			}
			buf.rewind();
		}
		stop("write CharBuffer BB");
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
