package org.zoodb.test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.zoodb.jdo.api.ZooConfig;

public class Test_011_DbAdminInMemory extends Test_010_DbAdmin {

	@BeforeClass
	public static void setUpClass() {
		ZooConfig.setFileManager(ZooConfig.FILE_MGR_IN_MEMORY);
	}
	
	//Test are in super-class
	
	@AfterClass
	public static void tearDownClass() {
		ZooConfig.setDefaults();
	}
}
