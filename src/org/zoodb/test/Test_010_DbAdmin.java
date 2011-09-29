package org.zoodb.test;

import javax.jdo.JDOUserException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.DataStoreManager;
import org.zoodb.jdo.api.ZooHelper;

import static junit.framework.TestCase.*;

public class Test_010_DbAdmin {

	private static final String dbName1 = "TestDb1";
	private static final String dbName2 = "TestDb2";

	@BeforeClass
	public static void setUpClass() {
		if (!dsm().repositoryExists()) {
			dsm().createDbRepository();
		}
		//remove all databases
		tearDownClass();
	}

	private static DataStoreManager dsm() {
		return ZooHelper.getDataStoreManager();
	}

	@Test
	public void testCreateDb() {
		//now files stuff should fail
		try {
			dsm().removeDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}


		dsm().createDb(dbName1);

		//now files stuff should fail
		try {
			dsm().createDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}

		dsm().removeDb(dbName1);

		//now files stuff should fail
		try {
			dsm().removeDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}

		//try again
		dsm().createDb(dbName1);
		dsm().removeDb(dbName1);
	}

	@Test
	public void testDoubleDbCreation() {
		//test files creation
		dsm().createDb(dbName1);
		try {
			dsm().createDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		dsm().createDb(dbName2);


		//test files removal
		dsm().removeDb(dbName1);
		try {
			dsm().removeDb(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		dsm().removeDb(dbName2);
	}


	@AfterClass
	public static void tearDownClass() {
		try {
			dsm().removeDb(dbName1);
			System.out.println("Removed files: " + dbName1);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			dsm().removeDb(dbName2);
			System.out.println("Removed files: " + dbName2);
		} catch (JDOUserException e) {
			//ok
		}
	}
}
