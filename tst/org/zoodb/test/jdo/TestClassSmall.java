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

	public TestClassTiny getRefP() {
		zooActivateRead();
		return refP;
	}

	public void setRefP(TestClassTiny refP) {
		zooActivateWrite();
		this.refP = refP;
	}
}
