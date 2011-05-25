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
	
	private static final DataStoreManager dataStoreManager = ZooHelper.getDataStoreManager();
	
	@BeforeClass
	public static void setUpClass() {
		if (!dataStoreManager.repositoryExists()) {
			dataStoreManager.createDbRepository();
		}
		//remove all databases
		tearDownClass();
	}
	
	
	@Test
	public void testCreateDb() {
		System.out.println("Testing DB creation");
		
		//test dir creation
		dataStoreManager.createDbFolder(dbName1);
		try {
			dataStoreManager.createDbFolder(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		dataStoreManager.createDbFolder(dbName2);
		

		subTestFileCreation();
		//test 2nd time to ensure that there are no conflicts from 1st call.
		subTestFileCreation();
		
		
		
		//test dir removal
		dataStoreManager.removeDbFolder(dbName1);
		try {
			dataStoreManager.removeDbFolder(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		dataStoreManager.removeDbFolder(dbName2);
		
		
		
		//now files stuff should fail
		try {
			dataStoreManager.createDbFiles(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		//now files stuff should fail
		try {
			dataStoreManager.removeDbFiles(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		
		//try again
		dataStoreManager.createDbFolder(dbName1);
		dataStoreManager.createDbFiles(dbName1);
		dataStoreManager.removeDbFiles(dbName1);
		dataStoreManager.removeDbFolder(dbName1);
	}
	
	
	private void subTestFileCreation() {
		//test files creation
		dataStoreManager.createDbFiles(dbName1);
		try {
			dataStoreManager.createDbFiles(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		dataStoreManager.createDbFiles(dbName2);

		
		//test files removal
		dataStoreManager.removeDbFiles(dbName1);
		try {
			dataStoreManager.removeDbFiles(dbName1);
			fail();
		} catch (JDOUserException e) {
			//ok
		}
		dataStoreManager.removeDbFiles(dbName2);
	}
	
	
	@AfterClass
	public static void tearDownClass() {
		try {
			dataStoreManager.removeDbFiles(dbName1);
			System.out.println("Removed files: " + dbName1);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			dataStoreManager.removeDbFiles(dbName2);
			System.out.println("Removed files: " + dbName2);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			dataStoreManager.removeDbFolder(dbName1);
			System.out.println("Removed folder: " + dbName1);
		} catch (JDOUserException e) {
			//ok
		}
		try {
			dataStoreManager.removeDbFolder(dbName2);
			System.out.println("Removed folder: " + dbName2);
		} catch (JDOUserException e) {
			//ok
		}
	}
}
