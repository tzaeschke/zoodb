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

import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooDebug;

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
	    TestTools.closePM();
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
