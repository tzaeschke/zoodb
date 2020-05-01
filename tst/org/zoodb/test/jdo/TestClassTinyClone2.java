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

import org.junit.Assert;

public class TestClassTinyClone2 extends TestClassTinyClone {


	private int i2;
	private long l2;
	
	public TestClassTinyClone2() {
		super();
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
