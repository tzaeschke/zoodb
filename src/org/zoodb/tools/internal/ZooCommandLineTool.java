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
package org.zoodb.tools.internal;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.zoodb.tools.ZooQuery;

public abstract class ZooCommandLineTool {

	protected static PrintStream out = System.out;
	protected static PrintStream err = System.err;
	
	private static final ByteArrayOutputStream BA = new ByteArrayOutputStream();

	
	public static void enableStringOutput() {
		ZooQuery.out = new PrintStream(BA);
	}
	
	public static String getStringOutput() {
		return BA.toString(); 
	}

	public static void resetStringOutput() {
		BA.reset();
	}
}
