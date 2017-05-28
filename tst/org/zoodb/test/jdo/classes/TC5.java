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

public class TC5 extends TC4 {
	
	private int i5;

	public TC5() {
		//empty
	}
	
	public TC5(int i0, int i1, int i2, int i3, int i4, int i5) {
		super(i0, i1, i2, i3, i4);
		this.i5 = i5;
	}

	public int getI5() {
		return i5;
	}

	public void setI5(int i5) {
		this.i5 = i5;
	}
	
}
