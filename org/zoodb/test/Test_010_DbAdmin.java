package org.zoodb.test;

import javax.jdo.JDOUserException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.custom.DataStoreManager;
import static junit.framework.TestCase.*;

public class Test_010_DbAdmin {

	private static final String dbName1 = "TestDb1";
	private static final String dbName2 = "TestDb2";
	
	@BeforeClass
	public static void setUpClass() {
		if (!DataStoreManager.repositoryExists()) {
			DataStoreManager.createDbRepository();
		}
		//remove all databases
		tearDownClass();
	}
	
	
	@Test
	public void testCreateDb() {
		System.out.println("Testing DB creation");
		
		//test dir creation
		DataStoreManager.createDbFolder(dbName1);
		try {
			DataStoreManager.createDbFolder(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		DataStoreManager.createDbFolder(dbName2);
		

		subTestFileCreation();
		//test 2nd time to ensure that there are no conflicts from 1st call.
		subTestFileCreation();
		
		
		
		//test dir removal
		DataStoreManager.removeDbFolder(dbName1);
		try {
			DataStoreManager.removeDbFolder(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		DataStoreManager.removeDbFolder(dbName2);
		
		
		
		//now files stuff should fail
		try {
			DataStoreManager.createDbFiles(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		//now files stuff should fail
		try {
			DataStoreManager.removeDbFiles(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		
		//try again
		DataStoreManager.createDbFolder(dbName1);
		DataStoreManager.createDbFiles(dbName1);
		DataStoreManager.removeDbFiles(dbName1);
		DataStoreManager.removeDbFolder(dbName1);
	}
	
	
	private void subTestFileCreation() {
		//test files creation
		DataStoreManager.createDbFiles(dbName1);
		try {
			DataStoreManager.createDbFiles(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		DataStoreManager.createDbFiles(dbName2);

		
		//test files removal
		DataStoreManager.removeDbFiles(dbName1);
		try {
			DataStoreManager.removeDbFiles(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		DataStoreManager.removeDbFiles(dbName2);
	}
	
	
	@AfterClass
	public static void tearDownClass() {
		try {
			DataStoreManager.removeDbFiles(dbName1);
			System.out.println("Removed files: " + dbName1);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			DataStoreManager.removeDbFiles(dbName2);
			System.out.println("Removed files: " + dbName2);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			DataStoreManager.removeDbFolder(dbName1);
			System.out.println("Removed folder: " + dbName1);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			DataStoreManager.removeDbFolder(dbName2);
			System.out.println("Removed folder: " + dbName2);
		} catch (JDOUserException e) {
			//ok
		}
	}
}
