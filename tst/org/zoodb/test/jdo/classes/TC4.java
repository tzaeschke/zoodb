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
package org.zoodb.test.jdo.classes;

public class TC4 extends TC3 {
	
	private int i4;

	public TC4() {
		//empty
	}
	
	public TC4(int i0, int i1, int i2, int i3, int i4) {
		super(i0, i1, i2, i3);
		this.i4 = i4;
	}

	public int getI4() {
		return i4;
	}

	public void setI4(int i4) {
		this.i4 = i4;
	}
	
}
