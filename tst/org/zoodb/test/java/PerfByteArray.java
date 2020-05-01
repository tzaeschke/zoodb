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

/**
 * Compare performance of direct buffer vs non-direct buffer.
 * 
 * Result direct buffers are 3-4 times faster for writing and 20 times faster for reading.
 * Creation may take 2 times longer for direct buffers.
 * 
 * @author Tilmann Zaeschke
 *
 */
public class PerfByteArray {
	
	private static final int MAX_J = 10000;
	
	public static void main(String[] args) {
		new PerfByteArray().run();
	}

	private void run() {
		createArrays();
		createArrays();
		createArrays();
		
		writeByte();
		writeByte();
		writeByte();

		writeLong();
		writeLong();
		writeLong();
		
		readByte();
		readByte();
		readByte();

		readLong();
		readLong();
		readLong();
	}
	
	private void createArrays() {
		start("create N");
		for (int i = 0; i < 10000; i++) {
			ByteBuffer.allocate(16000);
		}
		stop("create N");
		start("create D");
		for (int i = 0; i < 10000; i++) {
			ByteBuffer.allocateDirect(16000);
		}
		stop("create D");
	}
	
	private void writeByte() {
		ByteBuffer bbN = ByteBuffer.allocate(160000);
		ByteBuffer bbD = ByteBuffer.allocateDirect(160000);
		byte b = 10;
		start("write b N");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < 10000; i++) {
				bbN.put(b);
			}
			bbN.rewind();
		}
		stop("write b N");
		start("write b D");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < 10000; i++) {
				bbD.put(b);
			}
			bbD.rewind();
		}
		stop("write b D");
	}
	
	private void writeLong() {
		ByteBuffer bbN = ByteBuffer.allocate(160000);
		ByteBuffer bbD = ByteBuffer.allocateDirect(160000);
		start("write l N");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < 10000; i++) {
				bbN.putLong(10);
			}
			bbN.rewind();
		}
		stop("write l N");
		start("write l D");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < 10000; i++) {
				bbD.putLong(10);
			}
			bbD.rewind();
		}
		stop("write l D");
		
	}
	
	private void readByte() {
		ByteBuffer bbN = ByteBuffer.allocate(160000);
		ByteBuffer bbD = ByteBuffer.allocateDirect(160000);
		byte b = 10;
		start("read b N");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < 10000; i++) {
				bbN.get(b);
			}
			bbN.rewind();
		}
		stop("read b N");
		start("read b D");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < 10000; i++) {
				bbD.get();
			}
			bbD.rewind();
		}
		stop("read b D");
	}
	
	private void readLong() {
		ByteBuffer bbN = ByteBuffer.allocate(160000);
		ByteBuffer bbD = ByteBuffer.allocateDirect(160000);
		start("read l N");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < 10000; i++) {
				bbN.getLong();
			}
			bbN.rewind();
		}
		stop("read l N");
		start("read l D");
		for (int j = 0; j < MAX_J; j++) {
			for (int i = 0; i < 10000; i++) {
				bbD.getLong();
			}
			bbD.rewind();
		}
		stop("read l D");
		
	}
	
	private long time;
	private void start(String msg) {
		time = System.currentTimeMillis();
	}
	private void stop(String msg) {
		long t = System.currentTimeMillis() - time;
		double td = t/1000.0;
		System.out.println(msg + ": " + td);
	}
}
