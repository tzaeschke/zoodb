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

public class TestClassTiny2 extends TestClassTiny {


	private int i2;
	private long l2;
	
	public TestClassTiny2() {
		super();
	}
	
	public TestClassTiny2(int i1, long l1, int i2, long l2) {
		super(i1, l1);
		this.i2 = i2;
		this.l2 = l2;
	}

	public void setData2(int i, long l) {
        zooActivateWrite();
		i2 = i;
		l2 = l;
	}
	
	public void setInt2(int i) {
        zooActivateWrite();
		i2 = i;
	}
	
	public void setLong2(long l) {
        zooActivateWrite();
		l2 = l;
	}
	
	public void checkData2(int i, long l) {
        zooActivateRead();
		Assert.assertEquals(i, i2);
		Assert.assertEquals(l, l2);
	}
	
	public long getLong2() {
        zooActivateRead();
		return l2;
	}
	public int getInt2() {
        zooActivateRead();
		return i2;
	}
}
