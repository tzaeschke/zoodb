/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.profiler.test;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class TestClassTiny extends PersistenceCapableImpl {

	private int _int;
	private long _long;
	
	public void setInt(int i) {
        activateWrite("_int");
		_int = i;
	}
	
	public void setLong(long l) {
        activateWrite("_long");
		_long = l;
	}
	
	public long getLong() {
        activateRead("_long");
		return _long;
	}

	public int getInt() {
        activateRead("_int");
		return _int;
	}
}
