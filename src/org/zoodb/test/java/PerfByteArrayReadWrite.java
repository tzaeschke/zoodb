package org.zoodb.test.java;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

/**
 * Compare performance of different ways to write and read long[] from a byte-buffer. This is
 * for example used when reading and writing index pages.
 * 
 * Result for byte[]: The difference is quite small and almost negligible, writing 100.000 pages 
 * takes 0.x second. Interestingly, the writing gets a lot slower when using a direct buffer and 
 * loop based writing (buf.put(byte);), but still is just 0.1 secs.
 * -> use array based read/write
 * 
 * For int[], it's different, here loop-based is 1.5x/4x times faster (write/read) than array based.
 * 
 * Longs also do perform like ints, even though we don't have to create a temporary array.
 * 
 * For real world perf, it should be considered that loop based also always checks (autoPaging),
 * but that check should be fast.
 * 
 * 
 * @author Tilmann Zäschke
 */
public class PerfByteArrayReadWrite {
	
	private static final int MAX_J = 100000;
	private static final int MAX_N = 500;
	//use multiple arrays to avoid moving 'new byte[]' outside loop
	private static final int N_ARRAY = 8;
	private static final long[][] allValues = new long[N_ARRAY][MAX_N];
	private static final ByteBuffer buf = ByteBuffer.allocateDirect(MAX_N*8);
	private boolean autoPaging = false;
	
	
	public static void main(String[] args) {
		for (int j = 0; j < N_ARRAY; j++) {
			for (int i = 0; i < allValues[j].length; i++) {
				allValues[j][i] = i%200; 
			}
		}
		new PerfByteArrayReadWrite().run();
	}

	private void run() {
		writeByte();
		writeByte();
		writeByte();
		writeByte();

		writeInt();
		writeInt();
		writeInt();
		writeInt();

		writeLong();
		writeLong();
		writeLong();
		writeLong();
		
		readByte();
		readByte();
		readByte();
		readByte();

		readInt();
		readInt();
		readInt();
		readInt();

		readLong();
		readLong();
		readLong();
		readLong();
	}
	
	private void writeByte() {
		start("write b array");
		autoPaging = false;
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			byte[] ba = new byte[values.length];
			for (int i = 0; i < values.length; i++) {
				ba[i] = (byte) values[i];
			}
			buf.put(ba);
			buf.rewind();
		}
		autoPaging = true;
		stop("write b array");
		start("write b loop");
		autoPaging = false;
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			for (int i = 0; i < values.length; i++) {
				checkPos(4);
				buf.put((byte) values[i]);
			}
			buf.rewind();
		}
		autoPaging = true;
		stop("write b loop");
	}
	
	private void writeInt() {
		start("write i array");
		autoPaging = false;
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			int[] ba = new int[values.length];
			for (int i = 0; i < values.length; i++) {
				ba[i] = (byte) values[i];
			}
		    IntBuffer lb = buf.asIntBuffer();
		    lb.put(ba);
		    buf.position(buf.position() + 4 * ba.length);
			buf.rewind();
		}
		autoPaging = true;
		stop("write i array");
		start("write i loop");
		autoPaging = false;
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			for (int i = 0; i < values.length; i++) {
				checkPos(4);
				buf.putInt((int) values[i]);
			}
			buf.rewind();
		}
		autoPaging = true;
		stop("write i loop");
	}
	
	private void writeLong() {
		start("write l array");
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
		    LongBuffer lb = buf.asLongBuffer();
		    lb.put(values);
		    buf.position(buf.position() + 8 * values.length);
			buf.rewind();
		}
		stop("write l array");
		start("write l loop");
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			for (int i = 0; i < values.length; i++) {
				buf.putLong(values[i]);
			}
			buf.rewind();
		}
		stop("write l loop");
	}
	
	private void readByte() {
		start("read b array");
		autoPaging = false;
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			byte[] ba = new byte[values.length];
			buf.get(ba);
			for (int i = 0; i < values.length; i++) {
				values[i] = ba[i];
			}
			buf.rewind();
		}
		autoPaging = true;
		stop("read b array");
		start("read b loop");
		autoPaging = false;
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			for (int i = 0; i < values.length; i++) {
				checkPos(4);
				values[i] = buf.get();
			}
			buf.rewind();
		}
		autoPaging = true;
		stop("read b loop");
	}
	
	private void readInt() {
		start("read i array");
		autoPaging = false;
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			int[] ba = new int[values.length];
			IntBuffer lb = buf.asIntBuffer();
			lb.get(ba);
		    buf.position(buf.position() + 4 * ba.length);
			for (int i = 0; i < values.length; i++) {
				values[i] = ba[i];
			}
			buf.rewind();
		}
		autoPaging = true;
		stop("read i array");
		start("read i loop");
		autoPaging = false;
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			for (int i = 0; i < values.length; i++) {
				checkPos(4);
				values[i] = buf.getInt();
			}
			buf.rewind();
		}
		autoPaging = true;
		stop("read i loop");
	}
	
	private void readLong() {
		start("read l array");
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			LongBuffer lb = buf.asLongBuffer();
			lb.get(values);
		    buf.position(buf.position() + 8 * values.length);
			buf.rewind();
		}
		stop("read l array");
		start("read l loop");
		for (int j = 0; j < MAX_J; j++) {
			long[] values = allValues[j%N_ARRAY];
			for (int i = 0; i < values.length; i++) {
				values[i] = buf.getLong();
			}
			buf.rewind();
		}
		stop("read l loop");
		
	}
	
	private boolean checkPos(int l) {
		if (autoPaging) {
			throw new RuntimeException("" + l);
		}
		return autoPaging;
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
