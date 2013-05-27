/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.tools.internal;

import java.util.Scanner;

public class XmlReader {
	
	private String in;
	private int pos = 0;
	private final Scanner scanner;
	
	public XmlReader(Scanner scanner) {
		this.scanner = scanner;
	}

	private byte getByte() {
		char s1 = in.charAt(pos++);
		char s2 = in.charAt(pos++);
		byte b1 = (byte) (s1 > 64 ? s1-97+10 : s1-48); //assuming small letters
		byte b2 = (byte) (s2 > 64 ? s2-97+10 : s2-48);
		return (byte) ((b1<<4)+b2);
	}
	
	public String readString() {
		int len = readInt();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			byte s1 = getByte();
			byte s2 = getByte();
			char c = (char) ((s1 << 8) | s2);
			sb.append(c);
		}
		return sb.toString();
	}

	public boolean readBoolean() {
		return getByte() == 1;
	}

	public byte readByte() {
		return getByte();
	}

	public void readFully(byte[] ba) {
		for (int i = 0; i < ba.length; i++) {
			ba[i] = getByte();
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
		return (getByte()<<24) | (getByte()<<16) | (getByte()<<8) | getByte();
	}

	public long readLong() {
		return (((long)readInt()) << 32) | readInt();
	}

	public short readShort() {
		return (short) ((getByte()<<16) | getByte());
	}

	public void startReadingField(int fieldPos) {
		readln1("<attr");
		long id = Long.parseLong(readValue1("id"));
		if (id != fieldPos) {
			throw new IllegalStateException("Expected id: " + fieldPos + " but was " + id);
		}
		String value = readValue1("value");
		in = value;
		pos = 0;
	}

	public void stopReadingField() {
		readln1("/>");
	}

	/**
	 * Read a value, e.g. class="x.y" return "x.y" for read("class").
	 * @param name
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
