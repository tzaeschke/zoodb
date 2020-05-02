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
package org.zoodb.tools.internal;

import java.util.Scanner;

public class XmlReader {
	
	private String in;
	private int pos = 0;
	private final Scanner scanner;
	
	public XmlReader(Scanner scanner) {
		this.scanner = scanner;
	}

	private long getByte() {
		char s1 = in.charAt(pos++);
		char s2 = in.charAt(pos++);
		long b1 = (s1 > 64 ? s1-97+10 : s1-48); //assuming small letters
		long b2 = (s2 > 64 ? s2-97+10 : s2-48);
		//System.out.println("getByte: " + ((b1<<4)|b2) + " " + (byte)((b1<<4)|b2) + "  --" + s1 + s2);
		return ((b1<<4)|b2);
	}
	
	public String readString() {
		int len = readInt();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			long s1 = getByte();
			long s2 = getByte();
			char c = (char) ((s1 << 8) | s2);
			sb.append(c);
		}
		return sb.toString();
	}

	public boolean readBoolean() {
		return getByte() == 1;
	}

	public byte readByte() {
		return (byte) getByte();
	}

	public void readFully(byte[] ba) {
		for (int i = 0; i < ba.length; i++) {
			ba[i] = (byte) getByte();
		}
	}

	public char readChar() {
		return (char) ((getByte()<<8) | getByte());
	}

	public double readDouble() {
		long l = readLong();
		return Double.longBitsToDouble(l);
	}

	public float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	public int readInt() {
		int i = 0;
		i |= getByte();
		i <<= 8;
		i |= getByte();
		i <<= 8;
		i |= getByte();
		i <<= 8;
		i |= getByte();
		return i;
		//return (getByte()<<24) | (getByte()<<16) | (getByte()<<8) | getByte();
	}

	public long readLong() {
		return (getByte()<<56) | (getByte()<<48) | (getByte()<<40) | (getByte()<<32) | 
				(getByte()<<24) | (getByte()<<16) | (getByte()<<8) | getByte();
	}

	public short readShort() {
		return (short) ((getByte()<<8) | getByte());
	}

	public void startReadingField(int fieldPos) {
		readln1("<attr");
		long id = Long.parseLong(readValue1("id"));
		if (id != fieldPos) {
			throw new IllegalStateException("Expected id: " + fieldPos + " but was " + id);
		}
		in = readValue1("value");
		pos = 0;
	}

	public void stopReadingField() {
		readln1("/>");
	}

	/**
	 * Read a value, e.g. class="x.y" return "x.y" for read("class").
	 * @param name name
	 * @return value.
	 */
	private String readValue1(String name) {
		String in = scanner.next();
		if (!in.startsWith(name)) {
			throw new IllegalStateException("Expected " + name + " but got " + in);
		}
		if (in.endsWith(">")) {
			return in.substring(name.length() + 2, in.length()-2);
		} else {
			return in.substring(name.length() + 2, in.length()-1);
		}
	}

	private void readln1(String str) {
		String s2 = scanner.next();
		if (!s2.equals(str)) {
			throw new IllegalStateException("Expected: " + str + " but got: " + s2);
		}
	}
}
