/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import java.io.Serializable;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * Serializable test class.
 * 
 * @author Tilmann Zäschke
 *
 */
public class TestClassSer extends PersistenceCapableImpl implements Serializable {

	/**  */
	private static final long serialVersionUID = 1L;
	
	private int myInt;
	private long myLong;
	private String myString;
	private int[] myInts;
	private Object refO;
	private TestClassSer refP;
	
	public int getMyInt() {
		zooActivateRead();
		return myInt;
	}

	public void setMyInt(int myInt) {
		zooActivateWrite();
		this.myInt = myInt;
	}

	public long getMyLong() {
		zooActivateRead();
		return myLong;
	}

	public void setMyLong(long myLong) {
		zooActivateWrite();
		this.myLong = myLong;
	}

	public String getMyString() {
		zooActivateRead();
		return myString;
	}

	public void setMyString(String myString) {
		zooActivateWrite();
		this.myString = myString;
	}

	public int[] getMyInts() {
		zooActivateRead();
		return myInts;
	}

	public void setMyInts(int[] myInts) {
		zooActivateWrite();
		this.myInts = myInts;
	}

	public Object getRefO() {
		zooActivateRead();
		return refO;
	}

	public void setRefO(Object refO) {
		zooActivateWrite();
		this.refO = refO;
	}

	public TestClassSer getRefP() {
		zooActivateRead();
		return refP;
	}

	public void setRefP(TestClassSer refP) {
		zooActivateWrite();
		this.refP = refP;
	}
}
