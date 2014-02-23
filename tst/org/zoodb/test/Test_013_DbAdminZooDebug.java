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
package org.zoodb.test;

import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.ZooDebug;
import org.zoodb.test.testutil.TestTools;

public class Test_013_DbAdminZooDebug {
	
	private static final String DB_NAME = "TestDb";
	private static boolean isDebugBuffer;
	
	@BeforeClass
	public static void setUp() {
		isDebugBuffer = ZooDebug.isTesting();
		ZooDebug.setTesting(true);
		TestTools.createDb(DB_NAME);
	}

	@Test
	public void testFailureReport() {
	    TestTools.openPM();
	    try {
	    	ZooDebug.closeOpenFiles();
	    	fail();
	    } catch (IllegalStateException e) {
	    	//good!
	    }
	}
	
	@Test
	public void testNonFailure() {
	    TestTools.openPM();
	    TestTools.closePM();
	    ZooDebug.closeOpenFiles();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
		ZooDebug.setTesting(isDebugBuffer);
	}
}
