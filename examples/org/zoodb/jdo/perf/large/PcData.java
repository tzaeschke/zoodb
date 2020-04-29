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
package org.zoodb.jdo.perf.large;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

@SuppressWarnings("unused")
public class PcData extends PersistenceCapableImpl {

	private float f1;
	private float f2;
	private String s1;
	private String s2;
	private String s3;
	private String s4;
	
	private PcData() {
		//for JDO
	}
	
	public PcData(String s1, String s2, String s3, String s4, float f1, float f2) {
		this.s1 = s1;
		this.s2 = s2;
		this.s3 = s3;
		this.s4 = s4;
		this.f1 = f1;
		this.f2 = f2;
	}

	public String getS1() {
		zooActivateRead();
		return s1;
	}
	
}
