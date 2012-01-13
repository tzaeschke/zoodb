package org.zoodb.test;

import junit.framework.Assert;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class TestClassTinyClone extends PersistenceCapableImpl {

	private int _int;
	private long _long;
	
	public void setData(int i, boolean flag, char c, byte b, short s, long l, String str, byte[] ba) {
		_int = i;
	}
	
	public void setInt(int i) {
		_int = i;
	}
	
	public void setLong(long l) {
		_long = l;
	}
	
	public void checkData(int i, boolean flag, char c, byte b, short s, long l, String str, byte[] ba) {
		Assert.assertEquals(i, _int);
	}
	
	public long getLong() {
		return _long;
	}
	public int getInt() {
		return _int;
	}
}
