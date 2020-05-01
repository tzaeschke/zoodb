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

import java.io.IOException;
import java.io.Writer;

public class XmlWriter {

	private final Writer out;
	
	
	public XmlWriter(Writer out) {
		this.out = out;
	}

	public void writeString(String s) {
		writeInt(s.length());
		for (int i = 0; i < s.length(); i++) {
			writeChar(s.charAt(i));
		}
	}

	public void writeBoolean(boolean b) {
		write(b ? "01" : "00");
	}

	public void writeByte(byte b) {
		writeHex(b, 1);
	}

	public void write(byte[] ba) {
		for (int i = 0; i < ba.length; i++) {
			writeByte(ba[i]);
		}
	}

	public void writeChar(char c) {
		writeHex(c, 2);
	}

	public void writeDouble(double d) {
		writeLong(Double.doubleToRawLongBits(d));
	}

	public void writeFloat(float f) {
		writeInt(Float.floatToRawIntBits(f));
	}

	public void writeInt(int i) {
		writeHex(i, 4);
	}

	public void writeLong(long l) {
		writeHex(l, 8);
	}

	public void writeShort(short s) {
		writeHex(s, 2);
	}

	public void startObject(long oid) {
		writeln("   <object oid=\"" + oid + "\">");
	}
	
	public void finishObject() {
		writeln("   </object>");
	}

	public void startField(int fieldPos) {
		write("    <attr id=\"" + fieldPos + "\" value=\"");
	}

	public void finishField() {
		writeln("\" />");
	}

	private void writeHex(long in, int bytesToWrite) {
        char[] buf = new char[16];
        int charPos = 16;
        bytesToWrite <<= 1; //1 byte = 2 digits
        for (int j = 0; j < bytesToWrite; j++) {
            buf[--charPos] = digits[(int) (in & 0xfL)];
            in >>>= 4;
        }

		try {
			out.write(buf, charPos, (16 - charPos));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void write(String str) {
		try {
			out.write(str);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void writeln(String str) {
		write(str + '\n');
	}
	
	private final static char[] digits = {
        '0' , '1' , '2' , '3' , '4' , '5' ,
        '6' , '7' , '8' , '9' , 'a' , 'b' ,
        'c' , 'd' , 'e' , 'f'};

}
