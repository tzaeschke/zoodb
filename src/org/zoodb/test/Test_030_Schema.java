package org.zoodb.test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.Schema;
import org.zoodb.jdo.custom.DataStoreManager;
import org.zoodb.jdo.internal.server.DiskAccessOneFile;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.data.JB0;
import org.zoodb.test.data.JB1;
import org.zoodb.test.data.JB2;
import org.zoodb.test.data.JB3;
import org.zoodb.test.data.JB4;
import org.zoodb.test.data.JdoIndexedPilot;
import org.zoodb.test.data.JdoPilot;

public class Test_030_Schema {

	private static final String DB_NAME = "TestDb";
	private static final int PAGE_SIZE = DiskAccessOneFile.PAGE_SIZE;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
	}

	@Before
	public void before() {
        TestTools.removeDb(DB_NAME);
        TestTools.createDb(DB_NAME);
	}
	
	@Test
	public void testSchemaCreation() {
		System.out.println("Testing Schemas");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		Schema s01 = Schema.locate(pm, TestClass.class.getName(), DB_NAME);
		Schema s02 = Schema.locate(pm, TestClass.class, DB_NAME);
		assertNull(s01);
		assertNull(s02);
		
		Schema.create(pm, TestClass.class, DB_NAME);
		
		Schema s1 = Schema.locate(pm, TestClass.class.getName(), DB_NAME);
		Schema s2 = Schema.locate(pm, TestClass.class, DB_NAME);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);
		
		pm.currentTransaction().commit();
		
		s1 = Schema.locate(pm, TestClass.class.getName(), DB_NAME);
		s2 = Schema.locate(pm, TestClass.class, DB_NAME);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);

		pm.close();
		TestTools.closePM();
		
		//new session
		pm = TestTools.openPM();

		
		s1 = Schema.locate(pm, TestClass.class.getName(), DB_NAME);
		s2 = Schema.locate(pm, TestClass.class, DB_NAME);
//		System.out.println("STUFF: " + s1 + "  -  " + s2);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);

		try {
			//creating an existing schema should fail
			Schema.create(pm, TestClass.class, DB_NAME);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaDeletion() {
		System.out.println("Testing Schema deletion");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Schema s01 = Schema.create(pm, TestClass.class, DB_NAME);
		assertNotNull(s01);
		s01.remove();
		assertNull( Schema.locate(pm, TestClass.class.getName(), DB_NAME) );
		
		//commit no schema
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//create and commit
		assertNull( Schema.locate(pm, TestClass.class.getName(), DB_NAME) );
		s01 = Schema.create(pm, TestClass.class, DB_NAME);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		s01.remove();
		try {
			s01.remove();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		assertNull( Schema.locate(pm, TestClass.class.getName(), DB_NAME) );
		
		//roll back
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		s01 = Schema.locate(pm, TestClass.class.getName(), DB_NAME);
		assertNotNull( s01 );

		//remove and commit
		s01.remove();
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//check that it is gone
		assertNull( Schema.locate(pm, TestClass.class.getName(), DB_NAME) );
		try {
			s01.remove();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//check recreation
		s01 = Schema.create(pm, TestClass.class, DB_NAME);
		assertNotNull( Schema.locate(pm, TestClass.class.getName(), DB_NAME) );
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
		
	@Test
	public void testPageAllocation() {
		//test that allocating 6 schemas does not require too many pages 
		String path = DataStoreManager.getDbPath(DB_NAME);
		File file = new File(path);
		assertTrue(file.exists());
		assertTrue(file.isFile());
		long len1 = file.length();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Schema.create(pm, JdoPilot.class, DB_NAME);
		Schema.create(pm, JB0.class, DB_NAME);
		Schema.create(pm, JB1.class, DB_NAME);
		Schema.create(pm, JB2.class, DB_NAME);
		Schema.create(pm, JB3.class, DB_NAME);
		Schema.create(pm, JB4.class, DB_NAME);
		Schema.create(pm, JdoIndexedPilot.class, DB_NAME);
		pm.currentTransaction().commit();
		TestTools.closePM();
		long len2 = file.length();
		int newPages = (int) ((len2-len1)/PAGE_SIZE);
		//Allow 10 new pages max:
		//- 7*2 for each object index
		//- +3 for random stuff??
		assertTrue("new pages: " + newPages, newPages <= 3*7 + 3);
	}
	
	@Test
	public void testSchemaHierarchy() {
		//test that allocating 6 schemas does not require too many pages 
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Schema.create(pm, JB0.class, DB_NAME);
		Schema.create(pm, JB1.class, DB_NAME);
		Schema.create(pm, JB2.class, DB_NAME);
		Schema.create(pm, JB3.class, DB_NAME);
		Schema.create(pm, JB4.class, DB_NAME);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		assertNotNull( Schema.locate(pm, JB0.class, DB_NAME) );
		assertNotNull( Schema.locate(pm, JB1.class, DB_NAME) );
		assertNotNull( Schema.locate(pm, JB2.class, DB_NAME) );
		assertNotNull( Schema.locate(pm, JB3.class, DB_NAME) );
		assertNotNull( Schema.locate(pm, JB4.class, DB_NAME) );
		
		JB4 jb4 = new JB4();
		pm.makePersistent(jb4);
		JB0 jb0 = new JB0();
		pm.makePersistent(jb0);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@Test
	public void testLargeSchema() {
        //test that allocating 6 schemas does not require too many pages 
        
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        Schema.create(pm, JB0.class, DB_NAME);
        Schema.create(pm, JB1.class, DB_NAME);
        Schema.create(pm, JB2.class, DB_NAME);
        Schema.create(pm, JB3.class, DB_NAME);
        Schema.create(pm, JB4.class, DB_NAME);
        Schema.create(pm, TestSerializer.class, DB_NAME);
        pm.currentTransaction().commit();
        TestTools.closePM();

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        assertNotNull( Schema.locate(pm, JB0.class, DB_NAME) );
        assertNotNull( Schema.locate(pm, JB1.class, DB_NAME) );
        assertNotNull( Schema.locate(pm, JB2.class, DB_NAME) );
        assertNotNull( Schema.locate(pm, JB3.class, DB_NAME) );
        assertNotNull( Schema.locate(pm, JB4.class, DB_NAME) );
        assertNotNull( Schema.locate(pm, TestSerializer.class, DB_NAME) );
        
        JB4 jb4 = new JB4();
        pm.makePersistent(jb4);
        JB0 jb0 = new JB0();
        pm.makePersistent(jb0);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
	}
	
	@Test
	public void testMakePersistent() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        TestClass tc = new TestClass();
        
        try {
            pm.makePersistent(tc);
            fail();
        } catch (JDOUserException e) {
            //good
        }

        //create schema
        Schema s01 = Schema.create(pm, TestClass.class, DB_NAME);
        pm.makePersistent(tc);
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        
        //delete schema
        s01.remove();
        
        try {
            pm.makePersistent(tc);
            fail();
        } catch (JDOUserException e) {
            //good
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        try {
            pm.makePersistent(tc);
            fail();
        } catch (JDOUserException e) {
            //good
        }
        
        pm.currentTransaction().rollback();
        TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
}
