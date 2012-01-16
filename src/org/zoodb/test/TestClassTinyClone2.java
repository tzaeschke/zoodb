package org.zoodb.test;

import junit.framework.Assert;

public class TestClassTinyClone2 extends TestClassTinyClone {


	private int i2;
	private long l2;
	
	public TestClassTinyClone2() {
		super();
	}
	
	public void setData2(int i, long l) {
		i2 = i;
		l2 = l;
	}
	
	public void setInt2(int i) {
		i2 = i;
	}
	
	public void setLong2(long l) {
		l2 = l;
	}
	
	public void checkData2(int i, long l) {
		Assert.assertEquals(i, i2);
		Assert.assertEquals(l, l2);
	}
	
	public long getLong2() {
		return l2;
	}
	public int getInt2() {
		return i2;
	}
}
