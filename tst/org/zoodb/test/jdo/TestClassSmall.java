/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.jdo;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class TestClassSmall extends PersistenceCapableImpl {

	private int myInt;
	private long myLong;
	private String myString;
	private int[] myInts;
	private Object refO;
	private TestClassTiny refP;
	
	public int getMyInt() {
		return myInt;
	}

	public void setMyInt(int myInt) {
		this.myInt = myInt;
	}

	public long getMyLong() {
		return myLong;
	}

	public void setMyLong(long myLong) {
		this.myLong = myLong;
	}

	public String getMyString() {
		return myString;
	}

	public void setMyString(String myString) {
		this.myString = myString;
	}

	public int[] getMyInts() {
		return myInts;
	}

	public void setMyInts(int[] myInts) {
		this.myInts = myInts;
	}

	public Object getRefO() {
		return refO;
	}

	public void setRefO(Object refO) {
		this.refO = refO;
	}

	public TestClassTiny getRefP() {
		return refP;
	}

	public void setRefP(TestClassTiny refP) {
		this.refP = refP;
	}
}
