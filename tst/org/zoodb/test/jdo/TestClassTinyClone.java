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

public class TestClassTinyClone extends PersistenceCapableImpl {

	private int _int;
	private long _long;
	
	public void setInt(int i) {
        zooActivateWrite();
		_int = i;
	}
	
	public void setLong(long l) {
        zooActivateWrite();
		_long = l;
	}
	
	public long getLong() {
        zooActivateRead();
		return _long;
	}
	public int getInt() {
        zooActivateRead();
		return _int;
	}
}
