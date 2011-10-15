package org.zoodb.test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;

import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.api.ZooHelper;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.internal.Config;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.data.JB0;
import org.zoodb.test.data.JB1;
import org.zoodb.test.data.JB2;
import org.zoodb.test.data.JB3;
import org.zoodb.test.data.JB4;
import org.zoodb.test.data.JdoIndexedPilot;
import org.zoodb.test.data.JdoPilot;
import org.zoodb.test.util.TestTools;

public class Test_030_Schema {

	private static final String DB_NAME = "TestDb";
	private static final int PAGE_SIZE = Config.getFilePageSize();
	
	@Before
	public void before() {
        TestTools.removeDb(DB_NAME);
        TestTools.createDb(DB_NAME);
	}
	
	@Test
	public void testSchemaCreation() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooSchema s01 = ZooSchema.locate(pm, TestClass.class.getName());
		ZooSchema s02 = ZooSchema.locate(pm, TestClass.class);
		assertNull(s01);
		assertNull(s02);
		
		ZooSchema.create(pm, TestClass.class);
		
		ZooSchema s1 = ZooSchema.locate(pm, TestClass.class.getName());
		ZooSchema s2 = ZooSchema.locate(pm, TestClass.class);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);
		
		pm.currentTransaction().commit();
		
		s1 = ZooSchema.locate(pm, TestClass.class.getName());
		s2 = ZooSchema.locate(pm, TestClass.class);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);

		pm.close();
		TestTools.closePM();
		
		//new session
		pm = TestTools.openPM();

		
		s1 = ZooSchema.locate(pm, TestClass.class.getName());
		s2 = ZooSchema.locate(pm, TestClass.class);
//		System.out.println("STUFF: " + s1 + "  -  " + s2);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);

		try {
			//creating an existing schema should fail
			ZooSchema.create(pm, TestClass.class);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaCreationWithNode() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooSchema s01 = ZooSchema.locate(pm, TestClass.class.getName(), DB_NAME);
		ZooSchema s02 = ZooSchema.locate(pm, TestClass.class, DB_NAME);
		assertNull(s01);
		assertNull(s02);
		
		ZooSchema.create(pm, TestClass.class, DB_NAME);
		
		ZooSchema s1 = ZooSchema.locate(pm, TestClass.class.getName(), DB_NAME);
		ZooSchema s2 = ZooSchema.locate(pm, TestClass.class, DB_NAME);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);
		
		pm.currentTransaction().commit();
		
		s1 = ZooSchema.locate(pm, TestClass.class.getName(), DB_NAME);
		s2 = ZooSchema.locate(pm, TestClass.class, DB_NAME);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);

		pm.close();
		TestTools.closePM();
		
		//new session
		pm = TestTools.openPM();

		
		s1 = ZooSchema.locate(pm, TestClass.class.getName(), DB_NAME);
		s2 = ZooSchema.locate(pm, TestClass.class, DB_NAME);
//		System.out.println("STUFF: " + s1 + "  -  " + s2);
		assertTrue(s1 == s2);
		assertTrue(s1.getSchemaClass() == TestClass.class);

		try {
			//creating an existing schema should fail
			ZooSchema.create(pm, TestClass.class, DB_NAME);
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaDeletion() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		ZooSchema s01 = ZooSchema.create(pm, TestClass.class);
		assertNotNull(s01);
		s01.remove();
		assertNull( ZooSchema.locate(pm, TestClass.class.getName()) );
		
		//commit no schema
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//create and commit
		assertNull( ZooSchema.locate(pm, TestClass.class.getName()) );
		s01 = ZooSchema.create(pm, TestClass.class);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		s01.remove();
		try {
			s01.remove();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		assertNull( ZooSchema.locate(pm, TestClass.class.getName()) );
		
		//roll back
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		s01 = ZooSchema.locate(pm, TestClass.class.getName());
		assertNotNull( s01 );

		//remove and commit
		s01.remove();
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//check that it is gone
		assertNull( ZooSchema.locate(pm, TestClass.class.getName()) );
		try {
			s01.remove();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//check recreation
		s01 = ZooSchema.create(pm, TestClass.class);
		assertNotNull( ZooSchema.locate(pm, TestClass.class.getName()) );
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
		
	@Test
	public void testPageAllocation() {
		//test that allocating 6 schemas does not require too many pages 
		String path = ZooHelper.getDataStoreManager().getDbPath(DB_NAME);
		File file = new File(path);
		assertTrue(file.exists());
		assertTrue(file.isFile());
		long len1 = file.length();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema.create(pm, JdoPilot.class);
		ZooSchema.create(pm, JB0.class);
		ZooSchema.create(pm, JB1.class);
		ZooSchema.create(pm, JB2.class);
		ZooSchema.create(pm, JB3.class);
		ZooSchema.create(pm, JB4.class);
		ZooSchema.create(pm, JdoIndexedPilot.class);
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
		ZooSchema.create(pm, JB0.class);
		ZooSchema.create(pm, JB1.class);
		ZooSchema.create(pm, JB2.class);
		ZooSchema.create(pm, JB3.class);
		ZooSchema.create(pm, JB4.class);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		assertNotNull( ZooSchema.locate(pm, JB0.class) );
		assertNotNull( ZooSchema.locate(pm, JB1.class) );
		assertNotNull( ZooSchema.locate(pm, JB2.class) );
		assertNotNull( ZooSchema.locate(pm, JB3.class) );
		assertNotNull( ZooSchema.locate(pm, JB4.class) );
		
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
        ZooSchema.create(pm, JB0.class);
        ZooSchema.create(pm, JB1.class);
        ZooSchema.create(pm, JB2.class);
        ZooSchema.create(pm, JB3.class);
        ZooSchema.create(pm, JB4.class);
        ZooSchema.create(pm, TestSerializer.class);
        pm.currentTransaction().commit();
        TestTools.closePM();

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        assertNotNull( ZooSchema.locate(pm, JB0.class) );
        assertNotNull( ZooSchema.locate(pm, JB1.class) );
        assertNotNull( ZooSchema.locate(pm, JB2.class) );
        assertNotNull( ZooSchema.locate(pm, JB3.class) );
        assertNotNull( ZooSchema.locate(pm, JB4.class) );
        assertNotNull( ZooSchema.locate(pm, TestSerializer.class) );
        
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
        ZooSchema s01 = ZooSchema.create(pm, TestClass.class);
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
	
	/**
	 * Check non-persistent capable classes.
	 */
	@Test
	public void testMakePersistentWithNPC() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        try {
        	//
            pm.makePersistent(new RuntimeException());
            fail();
        } catch (JDOUserException e) {
            //good
        }
		
        try {
        	//
            pm.getExtent(RuntimeException.class);
            fail();
        } catch (JDOUserException e) {
            //good
        }
		
        try {
        	//
            pm.newQuery(RuntimeException.class);
            fail();
        } catch (JDOUserException e) {
            //good
        }
		
        pm.currentTransaction().rollback();
        TestTools.closePM();
	}
	
	@Test
	public void testAutoCreateSchema() {
		ZooJdoProperties props = new ZooJdoProperties(DB_NAME);
		props.setAutoCreateSchema(true);
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();

        TestClass tc = new TestClass();

        assertNull(ZooSchema.locate(pm, TestClass.class));
        
        //do not create schema first!
        pm.makePersistent(tc);

        assertNotNull(ZooSchema.locate(pm, TestClass.class));

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        assertNotNull(ZooSchema.locate(pm, TestClass.class));
        
        //delete schema
        ZooSchema s01 = ZooSchema.locate(pm, TestClass.class);
        s01.remove();

        assertNull(ZooSchema.locate(pm, TestClass.class));
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        assertNull(ZooSchema.locate(pm, TestClass.class));
        
        pm.currentTransaction().rollback();
        pm.close();
        pmf.close();
        //TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
}
