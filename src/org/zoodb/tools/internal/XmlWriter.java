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

public class XmlWriter {

	private final StringBuilder out = new StringBuilder();
	
	public void writeString(String s) {
		writeInt(s.length());
		for (int i = 0; i < s.length(); i++) {
			//TODO use getBytes()?
			writeChar(s.charAt(0));
		}
		s.toCharArray();
		
	}

	public void writeBoolean(boolean b) {
		out.append(b ? "01" : "00");
	}

	public void writeByte(byte b) {
		out.append(Integer.toHexString(b).substring(6));
	}

	public void write(byte[] ba) {
		writeInt(ba.length);
		for (int i = 0; i < ba.length; i++) {
			out.append(Integer.toHexString(ba[i]).substring(6));
		}
	}

	public void writeChar(char c) {
		out.append(Integer.toHexString(c).substring(4));
	}

	public void writeDouble(double d) {
		out.append(Long.toHexString(Double.doubleToRawLongBits(d)));
	}

	public void writeFloat(float f) {
		out.append(Integer.toHexString(Float.floatToRawIntBits(f)));
	}

	public void writeInt(int i) {
		out.append(Integer.toHexString(i));
	}

	public void writeLong(long l) {
		out.append(Long.toHexString(l));
	}

	public void writeShort(short s) {
		out.append(Integer.toHexString(s).substring(4));
	}

}
