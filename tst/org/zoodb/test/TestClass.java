/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import org.junit.Assert;

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
		zooActivateWrite();
		_int = i;
	}
	
	public void setLong(long l) {
		zooActivateWrite();
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
		zooActivateRead();
		return _long;
	}
	public int getInt() {
		zooActivateRead();
		return _int;
	}
	public byte getByte() {
		zooActivateRead();
		return _byte;
	}
	public boolean getBool() {
		zooActivateRead();
		return _bool;
	}
	public short getShort() {
		zooActivateRead();
		return _short;
	}
	public float getFloat() {
		zooActivateRead();
		return _float;
	}
	public double getDouble() {
		zooActivateRead();
		return _double;
	}
	public char getChar() {
		zooActivateRead();
		return _char;
	}
	public String getString() {
		zooActivateRead();
		return _string;
	}
	public byte[] getBytaArray() {
		zooActivateRead();
		return _bArray;
	}

	public void setRef1(Object obj) {
		zooActivateWrite();
		_ref1 = obj;
	}

	public void setRef2(TestClass obj) {
		zooActivateWrite();
		_ref2 = obj;
	}
	
	public Object getRef1() {
		zooActivateRead();
		return _ref1;
	}
	
	public TestClass getRef2() {
		zooActivateRead();
		return _ref2;
	}

	public void setByteArray(byte[] ba) {
		zooActivateWrite();
		_bArray = ba;
	}

	public void setString(String string) {
		zooActivateWrite();
		_string = string;
	}

	public void setFloat(float f) {
		zooActivateWrite();
		_float = f;
	}

	public void setDouble(double d) {
		zooActivateWrite();
		_double = d;
	}

	public void setShort(short s) {
		zooActivateWrite();
		_short = s;
	}
}
