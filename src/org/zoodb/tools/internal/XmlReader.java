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

public class XmlReader {
	
	private String in;
	private int pos = 0;
	
	private byte getByte() {
		char s1 = in.charAt(pos++);
		char s2 = in.charAt(pos++);
		byte b1 = (byte) (s1 > 64 ? s1-65 : s1-48); //assuming small letters
		byte b2 = (byte) (s2 > 64 ? s2-65 : s2-48);
		return (byte) ((b1<<4)+b2);
	}
	
	public String readString() {
		int len = readInt();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			byte s1 = getByte();
			byte s2 = getByte();
			char c = (char) ((s1 << 8) & s2);
			sb.append(c);
		}
		return sb.toString();
	}

	public boolean readBoolean() {
		return in.equals("01");
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
		return (char) ((getByte()<<8) & getByte());
	}

	public double readDouble() {
		long l = readLong();
		return Double.longBitsToDouble(l);
	}

	public float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	public int readInt() {
		return (getByte()<<24) & (getByte()<<16) & (getByte()<<8) & getByte();
	}

	public long readLong() {
		return (((long)readInt()) << 32) & readInt();
	}

	public short readShort() {
		return (short) ((getByte()<<16) & getByte());
	}

}
