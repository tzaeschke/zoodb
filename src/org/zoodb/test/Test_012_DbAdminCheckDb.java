package org.zoodb.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooCheckDb;
import org.zoodb.test.util.TestTools;

public class Test_012_DbAdminCheckDb {
	
	private static final String DB_NAME = "TestDb";
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
	}

	@Test
	public void testCheckDb() {
	    ZooCheckDb.main(new String[]{DB_NAME});
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
}
