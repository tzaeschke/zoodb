package org.zoodb.test;

import junit.framework.Assert;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

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
	private byte[] _bArray;
	private Integer _intObj;
	private String _string;
	private Object _object;
	private Object _ref1;
	private TestClass _ref2;
	
	public void setData(int i, boolean flag, char c, byte b, short s, long l, String str, byte[] ba) {
		_int = i;
		_bool = flag;
		_long = l;
		_char = c;
		_byte = b;
		_short = s;
		_string = str;
		_bArray = ba;
	}
	
	public void setInt(int i) {
		_int = i;
	}
	
	public void setLong(long l) {
		_long = l;
	}
	
	public void checkData(int i, boolean flag, char c, byte b, short s, long l, String str, byte[] ba) {
		Assert.assertEquals(i, _int);
		Assert.assertEquals(flag, _bool);
		Assert.assertEquals(l, _long);
		Assert.assertEquals(c, _char);
		Assert.assertEquals(b, _byte);
		Assert.assertEquals(s, _short);
		Assert.assertEquals(str, _string);
		Assert.assertEquals(ba.length, _bArray.length);
		for (int n = 0; n < ba.length; n++) {
			Assert.assertEquals(ba[n], _bArray[n]);
		}
	}
	
	public long getLong() {
		return _long;
	}
	public int getInt() {
		return _int;
	}
	public byte getByte() {
		return _byte;
	}
	public boolean getBool() {
		return _bool;
	}
	public short getShort() {
		return _short;
	}
	public char getChar() {
		return _char;
	}
	public String getString() {
		return _string;
	}
	public byte[] getBytaArray() {
		return _bArray;
	}

	public void setRef1(Object obj) {
		_ref1 = obj;
	}

	public void setRef2(TestClass obj) {
		_ref2 = obj;
	}
	
	public Object getRef1() {
		return _ref1;
	}
	
	public TestClass getRef2() {
		return _ref2;
	}
	//TODO create test class in HCSS
}
