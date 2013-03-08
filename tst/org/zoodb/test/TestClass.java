/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.test;

import junit.framework.Assert;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

@SuppressWarnings("unused")
public class TestClass extends PersistenceCapableImpl {

	private transient int _transInt;
	private transient String _transString;
	private static int _staticInt;
	private static String _staticString;
	private int _int;
	private long _long;
	private boolean _bool;
	private char _char;
	private byte _byte;
	private short _short;
	private float _float;
	private double _double;
	private byte[] _bArray;
	private Integer _intObj;
	private String _string;
	private Object _object;
	private Object _ref1;
	private TestClass _ref2;
	
	public void setData(int i, boolean flag, char c, byte b, short s, long l, String str, 
			byte[] ba, float f, double d) {
		zooActivateWrite();
		_int = i;
		_bool = flag;
		_long = l;
		_char = c;
		_byte = b;
		_short = s;
		_float = f;
		_double = d;
		_string = str;
		_bArray = ba;
	}
	
	public void setInt(int i) {
		activateWrite("_int");
		_int = i;
	}
	
	public void setLong(long l) {
		activateWrite("_long");
		_long = l;
	}
	
	public void checkData(int i, boolean flag, char c, byte b, short s, long l, String str, 
			byte[] ba, float f, double d) {
		zooActivateRead();
		Assert.assertEquals(i, _int);
		Assert.assertEquals(flag, _bool);
		Assert.assertEquals(l, _long);
		Assert.assertEquals(c, _char);
		Assert.assertEquals(b, _byte);
		Assert.assertEquals(s, _short);
		Assert.assertEquals(str, _string);
		Assert.assertTrue(_float == f);
		Assert.assertTrue(_double == d);
		Assert.assertEquals(ba.length, _bArray.length);
		for (int n = 0; n < ba.length; n++) {
			Assert.assertEquals(ba[n], _bArray[n]);
		}
	}
	
	public long getLong() {
		activateRead("_long");
		return _long;
	}
	public int getInt() {
		activateRead("_int");
		return _int;
	}
	public byte getByte() {
		activateRead("_byte");
		return _byte;
	}
	public boolean getBool() {
		activateRead("_bool");
		return _bool;
	}
	public short getShort() {
		activateRead("_short");
		return _short;
	}
	public float getFloat() {
		activateRead("_float");
		return _float;
	}
	public double getDouble() {
		activateRead("_double");
		return _double;
	}
	public char getChar() {
		activateRead("_char");
		return _char;
	}
	public String getString() {
		activateRead("_string");
		return _string;
	}
	public byte[] getBytaArray() {
		activateRead("_bArray");
		return _bArray;
	}

	public void setRef1(Object obj) {
		activateWrite("_ref1");
		_ref1 = obj;
	}

	public void setRef2(TestClass obj) {
		activateWrite("_ref2");
		_ref2 = obj;
	}
	
	public Object getRef1() {
		activateRead("_ref1");
		return _ref1;
	}
	
	public TestClass getRef2() {
		activateRead("_ref2");
		return _ref2;
	}

	public void setByteArray(byte[] ba) {
		activateWrite("_bArray");
		_bArray = ba;
	}

	public void setString(String string) {
		activateWrite("_string");
		_string = string;
	}

	public void setFloat(float f) {
		activateWrite("_float");
		_float = f;
	}

	public void setDouble(double d) {
		activateWrite("_double");
		_double = d;
	}
}
