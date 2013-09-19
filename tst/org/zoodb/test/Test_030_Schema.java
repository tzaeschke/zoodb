/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Collection;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooConfig;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.api.impl.DBStatistics.STATS;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.data.JB0;
import org.zoodb.test.data.JB1;
import org.zoodb.test.data.JB2;
import org.zoodb.test.data.JB3;
import org.zoodb.test.data.JB4;
import org.zoodb.test.data.JdoIndexedPilot;
import org.zoodb.test.data.JdoPilot;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooHelper;

public class Test_030_Schema {

    private static final String DB_NAME = "TestDb";
    private static final int PAGE_SIZE = ZooConfig.getFilePageSize();

    @Before
    public void before() {
    	TestTools.closePM();
        TestTools.removeDb(DB_NAME);
        TestTools.createDb(DB_NAME);
    }

    @Test
    public void testSchemaCreation() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        ZooClass s01 = ZooSchema.locateClass(pm, TestClass.class.getName());
        ZooClass s02 = ZooSchema.locateClass(pm, TestClass.class);
        assertNull(s01);
        assertNull(s02);

        ZooSchema.defineClass(pm, TestClass.class);

        ZooClass s1 = ZooSchema.locateClass(pm, TestClass.class.getName());
        ZooClass s2 = ZooSchema.locateClass(pm, TestClass.class);
        assertTrue(s1 == s2);
        assertTrue(s1.getJavaClass() == TestClass.class);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        s1 = ZooSchema.locateClass(pm, TestClass.class.getName());
        s2 = ZooSchema.locateClass(pm, TestClass.class);
        assertTrue(s1 == s2);
        assertTrue(s1.getJavaClass() == TestClass.class);

        pm.currentTransaction().commit();
        pm.close();
        TestTools.closePM();

        //new session
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        s1 = ZooSchema.locateClass(pm, TestClass.class.getName());
        s2 = ZooSchema.locateClass(pm, TestClass.class);
        //      System.out.println("STUFF: " + s1 + "  -  " + s2);
        assertTrue(s1 == s2);
        assertTrue(s1.getJavaClass() == TestClass.class);

        try {
            //creating an existing schema should fail
            ZooSchema.defineClass(pm, TestClass.class);
            fail();
        } catch (JDOUserException e) {
            //good
        }

        pm.currentTransaction().commit();
        TestTools.closePM();
    }


    @Test
    public void testSchemaCreationWithHierarchy() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        try {
            ZooSchema.defineClass(pm, TestClassTiny2.class);
            fail();
        } catch (JDOUserException e) {
            //create super-schema first!
        }
        ZooSchema.defineClass(pm, TestClassTiny.class);
        ZooSchema.defineClass(pm, TestClassTiny2.class);

        ZooClass s2 = ZooSchema.locateClass(pm, TestClassTiny2.class);
        ZooClass s1 = ZooSchema.locateClass(pm, TestClassTiny.class);
        assertNotNull(s1);
        assertNotNull(s2);
        assertTrue(s1.getJavaClass() == TestClassTiny.class);
        assertTrue(s2.getJavaClass() == TestClassTiny2.class);

        pm.currentTransaction().commit();
        pm.close();
        TestTools.closePM();

        //new session
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        try {
            //creating an existing schema should fail
            ZooSchema.defineClass(pm, TestClassTiny2.class);
            fail();
        } catch (JDOUserException e) {
            //good
        }
        pm.currentTransaction().commit();
        TestTools.closePM();
    }


    @Test
    public void testSchemaCreationWithNode() {
    	System.out.println("For now we disable the node-aware tests.");
//        PersistenceManager pm = TestTools.openPM();
//        pm.currentTransaction().begin();
//
//        ZooClass s01 = ZooSchema.locateClass(pm, TestClass.class.getName(), DB_NAME);
//        ZooClass s02 = ZooSchema.locateClass(pm, TestClass.class, DB_NAME);
//        assertNull(s01);
//        assertNull(s02);
//
//        ZooSchema.defineClass(pm, TestClass.class, DB_NAME);
//
//        ZooClass s1 = ZooSchema.locateClass(pm, TestClass.class.getName(), DB_NAME);
//        ZooClass s2 = ZooSchema.locateClass(pm, TestClass.class, DB_NAME);
//        assertTrue(s1 == s2);
//        assertTrue(s1.getJavaClass() == TestClass.class);
//
//        pm.currentTransaction().commit();
//
//        s1 = ZooSchema.locateClass(pm, TestClass.class.getName(), DB_NAME);
//        s2 = ZooSchema.locateClass(pm, TestClass.class, DB_NAME);
//        assertTrue(s1 == s2);
//        assertTrue(s1.getJavaClass() == TestClass.class);
//
//        pm.close();
//        TestTools.closePM();
//
//        //new session
//        pm = TestTools.openPM();
//
//
//        s1 = ZooSchema.locateClass(pm, TestClass.class.getName(), DB_NAME);
//        s2 = ZooSchema.locateClass(pm, TestClass.class, DB_NAME);
//        //		System.out.println("STUFF: " + s1 + "  -  " + s2);
//        assertTrue(s1 == s2);
//        assertTrue(s1.getJavaClass() == TestClass.class);
//
//        try {
//            //creating an existing schema should fail
//            ZooSchema.defineClass(pm, TestClass.class, DB_NAME);
//            fail();
//        } catch (JDOUserException e) {
//            //good
//        }
//
//        TestTools.closePM();
    }


    @Test
    public void testSchemaCreationHierarchy() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        try {
            ZooSchema.defineClass(pm, TestClassTiny2.class);
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        ZooSchema.defineClass(pm, TestClassTiny.class);
        ZooSchema.defineClass(pm, TestClassTiny2.class);

        pm.currentTransaction().commit();
        TestTools.closePM();
    }        

    private class InnerClass extends ZooPCImpl {
        //
    }

    private static class StaticInnerClass extends ZooPCImpl {
        //
    }

    @Test
    public void testSchemaCreationInnerClass() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //member class
        try {
            ZooSchema.defineClass(pm, InnerClass.class);
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        //static inner class
        try {
            ZooSchema.defineClass(pm, StaticInnerClass.class);
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        //anonymous class
        Object anon = new ZooPCImpl() {}; 
        try {
            ZooSchema.defineClass(pm, anon.getClass());
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        //local class
        class LocalClass extends ZooPCImpl {};
        try {
            ZooSchema.defineClass(pm, LocalClass.class);
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        pm.currentTransaction().commit();
        TestTools.closePM();
    }        


    @Test
    public void testSchemaDeletion() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        ZooClass s01 = ZooSchema.defineClass(pm, TestClass.class);
        assertNotNull(s01);
        s01.remove();
        assertNull( ZooSchema.locateClass(pm, TestClass.class.getName()) );

        //commit no schema
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //create and commit
        assertNull( ZooSchema.locateClass(pm, TestClass.class.getName()) );
        s01 = ZooSchema.defineClass(pm, TestClass.class);
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        s01.remove();
        try {
            s01.remove();
            fail();
        } catch (IllegalStateException e) {
            //good
        }
        assertNull( ZooSchema.locateClass(pm, TestClass.class.getName()) );

        //roll back
        pm.currentTransaction().rollback();
        pm.currentTransaction().begin();
        s01 = ZooSchema.locateClass(pm, TestClass.class.getName());
        assertNotNull( s01 );

        //remove and commit
        s01.remove();
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //check that it is gone
        assertNull( ZooSchema.locateClass(pm, TestClass.class.getName()) );
        try {
            s01.remove();
            fail();
        } catch (IllegalStateException e) {
            //good
        }

        //check recreation
        s01 = ZooSchema.defineClass(pm, TestClass.class);
        assertNotNull( ZooSchema.locateClass(pm, TestClass.class.getName()) );

        pm.currentTransaction().commit();
        TestTools.closePM();
    }

    @Test
    public void testSchemaDeletionHierarchy() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        ZooClass s01 = ZooSchema.defineClass(pm, TestClassTiny.class);
        ZooClass s02 = ZooSchema.defineClass(pm, TestClassTiny2.class);
        try {
            //remove s02 first
            s01.remove();
            fail();
        } catch (IllegalStateException e) {
            //good
        }
        s02.remove();
        s01.remove();
        assertNull( ZooSchema.locateClass(pm, TestClassTiny.class.getName()) );
        assertNull( ZooSchema.locateClass(pm, TestClassTiny2.class.getName()) );

        //commit no schema
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //create and commit
        assertNull( ZooSchema.locateClass(pm, TestClassTiny.class.getName()) );
        assertNull( ZooSchema.locateClass(pm, TestClassTiny2.class.getName()) );
        s01 = ZooSchema.defineClass(pm, TestClassTiny.class);
        s02 = ZooSchema.defineClass(pm, TestClassTiny2.class);
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        s02.remove();
        s01.remove();
        try {
            s01.remove();
            fail();
        } catch (IllegalStateException e) {
            //good
        }
        try {
            s02.remove();
            fail();
        } catch (IllegalStateException e) {
            //good
        }
        assertNull( ZooSchema.locateClass(pm, TestClassTiny.class.getName()) );
        assertNull( ZooSchema.locateClass(pm, TestClassTiny2.class.getName()) );

        //roll back
        pm.currentTransaction().rollback();
        pm.currentTransaction().begin();
        s01 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
        s02 = ZooSchema.locateClass(pm, TestClassTiny2.class.getName());
        assertNotNull( s01 );
        assertNotNull( s02 );

        //remove and commit
        s02.remove();
        s01.remove();
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //check that it is gone
        assertNull( ZooSchema.locateClass(pm, TestClassTiny.class.getName()) );
        assertNull( ZooSchema.locateClass(pm, TestClassTiny2.class.getName()) );
        try {
            s01.remove();
            fail();
        } catch (IllegalStateException e) {
            //good
        }
        try {
            s02.remove();
            fail();
        } catch (IllegalStateException e) {
            //good
        }

        //check recreation
        s01 = ZooSchema.defineClass(pm, TestClassTiny.class);
        s02 = ZooSchema.defineClass(pm, TestClassTiny2.class);
        assertNotNull( ZooSchema.locateClass(pm, TestClassTiny.class.getName()) );
        assertNotNull( ZooSchema.locateClass(pm, TestClassTiny2.class.getName()) );

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
        ZooSchema.defineClass(pm, JdoPilot.class);
        ZooSchema.defineClass(pm, JB0.class);
        ZooSchema.defineClass(pm, JB1.class);
        ZooSchema.defineClass(pm, JB2.class);
        ZooSchema.defineClass(pm, JB3.class);
        ZooSchema.defineClass(pm, JB4.class);
        ZooSchema.defineClass(pm, JdoIndexedPilot.class);
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
        ZooSchema.defineClass(pm, JB0.class);
        ZooSchema.defineClass(pm, JB1.class);
        ZooSchema.defineClass(pm, JB2.class);
        ZooSchema.defineClass(pm, JB3.class);
        ZooSchema.defineClass(pm, JB4.class);
        pm.currentTransaction().commit();
        TestTools.closePM();

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        assertNotNull( ZooSchema.locateClass(pm, JB0.class) );
        assertNotNull( ZooSchema.locateClass(pm, JB1.class) );
        assertNotNull( ZooSchema.locateClass(pm, JB2.class) );
        assertNotNull( ZooSchema.locateClass(pm, JB3.class) );
        assertNotNull( ZooSchema.locateClass(pm, JB4.class) );

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
        ZooSchema.defineClass(pm, JB0.class);
        ZooSchema.defineClass(pm, JB1.class);
        ZooSchema.defineClass(pm, JB2.class);
        ZooSchema.defineClass(pm, JB3.class);
        ZooSchema.defineClass(pm, JB4.class);
        ZooSchema.defineClass(pm, TestSerializer.class);
        pm.currentTransaction().commit();
        TestTools.closePM();

        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        assertNotNull( ZooSchema.locateClass(pm, JB0.class) );
        assertNotNull( ZooSchema.locateClass(pm, JB1.class) );
        assertNotNull( ZooSchema.locateClass(pm, JB2.class) );
        assertNotNull( ZooSchema.locateClass(pm, JB3.class) );
        assertNotNull( ZooSchema.locateClass(pm, JB4.class) );
        assertNotNull( ZooSchema.locateClass(pm, TestSerializer.class) );

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
        ZooClass s01 = ZooSchema.defineClass(pm, TestClass.class);
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
        props.setZooAutoCreateSchema(true);
        PersistenceManagerFactory pmf = 
            JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();

        TestClass tc = new TestClass();

        assertNull(ZooSchema.locateClass(pm, TestClass.class));

        //do not create schema first!
        pm.makePersistent(tc);

        assertNotNull(ZooSchema.locateClass(pm, TestClass.class));

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        assertNotNull(ZooSchema.locateClass(pm, TestClass.class));

        //delete schema
        ZooClass s01 = ZooSchema.locateClass(pm, TestClass.class);
        s01.remove();

        assertNull(ZooSchema.locateClass(pm, TestClass.class));

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        assertNull(ZooSchema.locateClass(pm, TestClass.class));

        pm.currentTransaction().rollback();
        pm.close();
        pmf.close();
        //TestTools.closePM();
    }

    @Test
    public void testDropInstances() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //create schema
        ZooClass s01 = ZooSchema.defineClass(pm, TestClass.class);

        //test that it does not fail without instances or for a fresh schema
        s01.dropInstances();

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //test without instance
        s01.dropInstances();

        //create instance
        TestClass tc = new TestClass();
        pm.makePersistent(tc);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        int p1 = ZooHelper.getStatistics(pm).getStat(STATS.DB_PAGE_CNT_DATA);

        //delete instances
        s01.dropInstances();

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //test that pages are freed up.
        int p2 = ZooHelper.getStatistics(pm).getStat(STATS.DB_PAGE_CNT_DATA);
        assertTrue(p2 < p1);

        Collection<?> c = (Collection<?>) pm.newQuery(TestClass.class).execute();
        assertFalse(c.iterator().hasNext());

        Extent<?> ext = pm.getExtent(TestClass.class);
        assertFalse(ext.iterator().hasNext());
        ext.closeAll();
        
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }

    /**
     * This test reproduces a bug related to dropInstances
     */
    @Test
    public void testDropInstancesBug() {
    	//bug require > 100 instances of unrelated(!) class
    	final int N = 1000;

        //Init
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooClass r = ZooSchema.defineClass(pm, TestClass.class);
		r.getField("_int").createIndex(false);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		r = ZooSchema.locateClass(pm, TestClass.class);
		r.dropInstances();
		pm.currentTransaction().commit();

		//create data
		pm.currentTransaction().begin();
		for (int i = 0; i < N; i++) {
			TestClassTiny p = new TestClassTiny();
			p.setInt(1);
			pm.makePersistent(p);
		}
		pm.currentTransaction().commit();
		pm.close();
		
		
		//open/close is essential for bug --> THIS IS WHERE IT FAILED!
        pm = TestTools.openPM();
        pm.currentTransaction().begin();

        int n = 0;
		Query q = pm.newQuery(TestClass.class, "_int == 1"); 
		for (Object o: (Collection<?>)q.execute()) {
			assertTrue(o instanceof TestClass);
			n++;
		}
		q.closeAll();
		assertEquals(0, n);
        
        n = 0;
		q = pm.newQuery(TestClassTiny.class, "_int == 1"); 
		for (Object o: (Collection<?>)q.execute()) {
			assertTrue(o instanceof TestClassTiny);
			n++;
		}
		q.closeAll();
		assertEquals(N, n);
        
        pm.currentTransaction().rollback();
        pm.close();
    }

    @Test
    public void testAddRemoveReopen() {
        TestTools.defineSchema(TestClass.class);
        TestTools.removeSchema(TestClass.class);
        TestTools.openPM();
        TestTools.closePM();
    }

    @Test
    public void testSchemaName() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        //create schema
        ZooClass s01 = ZooSchema.defineClass(pm, TestClass.class);
        assertEquals(TestClass.class.getName(), s01.getName());
        
        TestTools.closePM();
    }

    @Test
    public void testSchemaGetAll() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        Collection<ZooClass> coll = ZooSchema.locateAllClasses(pm);
        assertEquals(5, coll.size());
        
        //create schema
        ZooClass s01 = ZooSchema.defineClass(pm, TestClass.class);
        assertEquals(TestClass.class.getName(), s01.getName());

        coll = ZooSchema.locateAllClasses(pm);
        assertEquals(6, coll.size());
        for (ZooClass cls: coll) {
            assertTrue(cls.getName().startsWith("org.zoodb."));
        }

        s01.remove();
        coll = ZooSchema.locateAllClasses(pm);
        assertEquals(5, coll.size());
        
        TestTools.closePM();
    }

    
    @Test 
    public void testNewlyCreatedSchema() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        ZooSchema.defineClass(pm, TestClass.class);
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        TestClass tc = new TestClass();
        tc.setString("xyz");
        pm.makePersistent(tc);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        Query q = pm.newQuery(TestClass.class);
        q.setFilter("_string == xx");
        q.declareParameters("String xx");
        long n = q.deletePersistentAll("xyz");
        assertEquals(1, n);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
       TestTools.closePM();
    }
    
    @After
    public void after() {
        TestTools.closePM();
    }

    @AfterClass
    public static void tearDown() {
        TestTools.removeDb(DB_NAME);
    }
}
