package org.zoodb.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.zoodb.jdo.internal.Config;

public class Test_011_DbAdminInMemory extends Test_010_DbAdmin {

	@BeforeClass
	public static void setUpClass() {
		Config.setFileManager(Config.FILE_MGR_IN_MEMORY);
	}
	
	//Test are in super-class
	
	@AfterClass
	public static void tearDownClass() {
		Config.setDefaults();
	}
}
