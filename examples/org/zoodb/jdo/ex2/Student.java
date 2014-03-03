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
package org.zoodb.jdo.ex2;

public class Student extends Person {

	private int id;
	
	private Student() {
		super(null);
		//just for ZooDB
	}
	
	public Student(String name, int id) {
		super(name);
		this.id = id;
	}
	
	@Override
	public String toString() {
		zooActivateRead();
		return super.toString() + " - " + id;
	}

	public int getID() {
		zooActivateRead();
		return id;
	}
}
