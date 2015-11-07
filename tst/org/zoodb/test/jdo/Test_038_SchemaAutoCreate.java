/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.jdo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.api.impl.ZooPC;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.testutil.TestTools;

public class Test_038_SchemaAutoCreate {

    private static ZooJdoProperties props;
    
    @BeforeClass
    public static void beforeClass() {
    	props = new ZooJdoProperties(TestTools.getDbName());
    	props.setZooAutoCreateSchema(true);
    }
    
    @Before
    public void before() {
    	TestTools.closePM();
        TestTools.removeDb();
        TestTools.createDb();
    }
    
    @Test
    public void testSchemaAutoCreation() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        ZooClass s01 = ZooJdoHelper.schema(pm).getClass(TestClass.class.getName());
        ZooClass s02 = ZooJdoHelper.schema(pm).getClass(TestClass.class);
        assertNull(s01);
        assertNull(s02);

        pm.makePersistent(new TestClass());

        ZooClass s1 = ZooJdoHelper.schema(pm).getClass(TestClass.class.getName());
        ZooClass s2 = ZooJdoHelper.schema(pm).getClass(TestClass.class);
        assertTrue(s1 == s2);
        assertTrue(s1.getJavaClass() == TestClass.class);

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        s1 = ZooJdoHelper.schema(pm).getClass(TestClass.class.getName());
        s2 = ZooJdoHelper.schema(pm).getClass(TestClass.class);
        assertTrue(s1 == s2);
        assertTrue(s1.getJavaClass() == TestClass.class);

        pm.currentTransaction().commit();
        pm.close();
        TestTools.closePM();

        //new session
        pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        s1 = ZooJdoHelper.schema(pm).getClass(TestClass.class.getName());
        s2 = ZooJdoHelper.schema(pm).getClass(TestClass.class);
        //      System.out.println("STUFF: " + s1 + "  -  " + s2);
        assertTrue(s1 == s2);
        assertTrue(s1.getJavaClass() == TestClass.class);

        try {
            //creating an existing schema should fail
            ZooJdoHelper.schema(pm).addClass(TestClass.class);
            fail();
        } catch (JDOUserException e) {
            //good
        }

        pm.currentTransaction().commit();
        TestTools.closePM();
    }

    @Test
    public void testSchemaAutoCreationHierarchy() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        pm.makePersistent(new TestClassSmallA());
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClassSmall.class.getName());
        ZooClass s1 = ZooJdoHelper.schema(pm).getClass(TestClassSmallA.class.getName());
        ZooClass s2 = ZooJdoHelper.schema(pm).getClass(TestClassSmallB.class.getName());
        ZooClass t = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class.getName());
        assertNotNull(s); //super class
        assertNotNull(s1);  //instantiated class
        assertNotNull(s2);  //referenced class
        assertNotNull(t);   //referenced in super
        
        pm.currentTransaction().commit();
        TestTools.closePM();
   }

    /**
     * Test that persisting class A with a reference to an SCO B <b>fails</b> if the according 
     * setting is enabled in ZooDB.
     */
    @Test
    public void testSchemaCreationChickenEgg() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        pm.makePersistent(new TestClassSmallA());
//        ZooSchema.defineClass(pm, TestClassTiny.class);
//        ZooSchema.defineClass(pm, TestClassSmall.class);
//        ZooSchema.defineClass(pm, TestClassSmallA.class);
        
        //should not fail depspite missing schemata
        pm.currentTransaction().commit();
        
        pm.currentTransaction().begin();
        try {
        	ZooJdoHelper.schema(pm).addClass(TestClassSmallB.class);
        	fail();
        } catch (JDOUserException e) {
        	//good. should fail because schema was already implicitly defined.
        }

        pm.currentTransaction().commit();
        pm.close();
        TestTools.closePM();

        //new session
        pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        ZooClass s = ZooJdoHelper.schema(pm).getClass(TestClassSmall.class.getName());
        ZooClass s1 = ZooJdoHelper.schema(pm).getClass(TestClassSmallA.class.getName());
        ZooClass s2 = ZooJdoHelper.schema(pm).getClass(TestClassSmallB.class.getName());

        s1.remove();
        
        try {
            pm.currentTransaction().commit();
            fail();
        } catch (JDOUserException e) {
        	//good, can't commit because s2 depends on s1
        	//Message should contain name of class and one referenced class
        	assertTrue(e.getMessage().contains(TestClassSmallA.class.getSimpleName()));
        	assertTrue(e.getMessage().contains(TestClassSmallB.class.getSimpleName()));
        }

        pm.currentTransaction().begin();
        s1.remove();
        s2.remove();
        s.remove();
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }

    @Test
    public void testSchemaCreationWithHierarchy() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        pm.makePersistent(new TestClassTiny2());
        
        ZooClass s2 = ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class);
        ZooClass s1 = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
        assertNotNull(s1);
        assertNotNull(s2);
        assertTrue(s1.getJavaClass() == TestClassTiny.class);
        assertTrue(s2.getJavaClass() == TestClassTiny2.class);

        pm.makePersistent(new TestClassTiny());
        
        pm.currentTransaction().commit();
        pm.close();
        TestTools.closePM();

        //new session
        pm = TestTools.openPM(props);
        pm.currentTransaction().begin();
        try {
            //creating an existing schema should fail
        	ZooJdoHelper.schema(pm).addClass(TestClassTiny2.class);
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
//        PersistenceManager pm = TestTools.openPM(props);
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
//        pm = TestTools.openPM(props);
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
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        pm.makePersistent(new TestClassTiny());
        pm.makePersistent(new TestClassTiny2());

        pm.currentTransaction().commit();
        TestTools.closePM();
    }        

    private class InnerClass extends ZooPC {
        //
    }

    private static class StaticInnerClass extends ZooPC {
        //
    }

    @Test
    public void testSchemaCreationInnerClass() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        //member class
        try {
        	pm.makePersistent(new InnerClass());
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        //static inner class
        try {
        	pm.makePersistent(new StaticInnerClass());
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        //anonymous class
        try {
        	pm.makePersistent(new ZooPC() {});
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        //local class
        class LocalClass extends ZooPC {};
        try {
        	pm.makePersistent(new LocalClass());
            fail();
        } catch (JDOUserException e) {
            //should fail
        }

        pm.currentTransaction().commit();
        TestTools.closePM();
    }        


    private class InnerClassSCO {
        //
    }

    private static class StaticInnerClassSCO {
        //
    }

    @Test
    public void testSchemaCreationInnerClassSCO() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        
        //member class
        TestClass tc1 = new TestClass();
        tc1.setRef1(new InnerClassSCO());
        pm.makePersistent(tc1);
        try {
        	pm.currentTransaction().commit();
            fail();
        } catch (JDOUserException e) {
            //should fail
        }
    	pm.currentTransaction().begin();

        //static inner class
        TestClass tc2 = new TestClass();
        tc2.setRef1(new StaticInnerClassSCO());
        pm.makePersistent(tc2);
        try {
        	pm.currentTransaction().commit();
            fail();
        } catch (JDOUserException e) {
            //should fail
        }
    	pm.currentTransaction().begin();

        //anonymous class
        TestClass tc3 = new TestClass();
        tc3.setRef1(new Object() {});
        pm.makePersistent(tc3);
        try {
        	pm.currentTransaction().commit();
            fail();
        } catch (JDOUserException e) {
            //should fail
        }
    	pm.currentTransaction().begin();

        //local class
        class LocalClass {};
        TestClass tc4 = new TestClass();
        tc4.setRef1(new LocalClass());
        pm.makePersistent(tc4);
        try {
        	pm.currentTransaction().commit();
            fail();
        } catch (JDOUserException e) {
            //should fail
        }
    	pm.currentTransaction().begin();

        pm.currentTransaction().commit();
        TestTools.closePM();
    }        


    /**
     * Check non-persistent capable classes.
     */
    @Test
    public void testMakePersistentWithNPC() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        try {
            //
            pm.makePersistent(new RuntimeException());
            fail();
        } catch (JDOUserException e) {
            //good
        }

        pm.currentTransaction().rollback();
        TestTools.closePM();
    }

    @Test
    public void testAutoCreateSchema() {
        ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
        props.setZooAutoCreateSchema(true);
        PersistenceManagerFactory pmf = 
            JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();

        TestClass tc = new TestClass();

        assertNull(ZooJdoHelper.schema(pm).getClass(TestClass.class));

        //do not create schema first!
        pm.makePersistent(tc);

        assertNotNull(ZooJdoHelper.schema(pm).getClass(TestClass.class));

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        assertNotNull(ZooJdoHelper.schema(pm).getClass(TestClass.class));

        //delete schema
        ZooClass s01 = ZooJdoHelper.schema(pm).getClass(TestClass.class);
        s01.remove();

        assertNull(ZooJdoHelper.schema(pm).getClass(TestClass.class));

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        assertNull(ZooJdoHelper.schema(pm).getClass(TestClass.class));

        pm.currentTransaction().rollback();
        pm.close();
        pmf.close();
        //TestTools.closePM();
    }

    @Test
    public void testQueryWithAuto() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        try {
            //
            pm.newQuery(RuntimeException.class);
            fail();
        } catch (JDOUserException e) {
            //good
        }

        Query q = pm.newQuery(TestClassTiny2.class);
        assertTrue(((Collection<?>)q.execute()).isEmpty());

        Query q2 = pm.newQuery("select from " + TestClassTiny.class.getName());
        assertTrue(((Collection<?>)q2.execute()).isEmpty());

        assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class));
        assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class));
        
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }

    /**
     * Bug #40: NPE in query compile().
     */
    @Test
    public void testQueryCompileWithAutoBug_40() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        Query q = pm.newQuery(TestClassTiny.class, "_int == 123");
        q.compile();
        
        assertTrue(((Collection<?>)q.execute()).isEmpty());

        assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class));
        assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class));
        
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }

    @Test
    public void testExtentWithAuto() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        try {
            //
            pm.getExtent(RuntimeException.class);
            fail();
        } catch (JDOUserException e) {
            //good
        }

        Extent<?> e = pm.getExtent(TestClassTiny2.class);
        assertFalse(e.iterator().hasNext());

        Extent<?> e2 = pm.getExtent(TestClassTiny.class, true);
        assertFalse(e2.iterator().hasNext());

        assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class));
        assertNull(ZooJdoHelper.schema(pm).getClass(TestClassTiny2.class));
        
        pm.currentTransaction().rollback();
        TestTools.closePM();
    }

    /**
     * Fails.
     * 
     * Requires 
     * - auto-creation of a schema
     * - rollback of auto-created schema
     */
    @Test
    public void testSchemaAutoCreationBug_Issue_54() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();

        pm.makePersistent(new TestClassTiny());
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        ZooClass t = ZooJdoHelper.schema(pm).getClass(TestClassTiny.class);
        t.createIndex("_int", true);
        
        //not really necessary to produce error...
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        TestClassTiny2 tt2 = new TestClassTiny2();
        pm.makePersistent(tt2);
        try {
            pm.currentTransaction().commit();
            fail();
        } catch (JDOUserException e) {
        	//Exactly, this is an index violation
            pm.currentTransaction().begin();
        }

        tt2.setInt(2);
        pm.makePersistent(tt2);

        //just another check, since the refresh() schema seemed to swallow the index information...
        //TODO The schemaToRefresh list in ClientSessionCache.rollback() is never used....
        assertTrue(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class).getField("_int").hasIndex());
        assertTrue(ZooJdoHelper.schema(pm).getClass(TestClassTiny.class).getField("_int").isIndexUnique());

        pm.currentTransaction().commit();
        
        TestTools.closePM();
   }

}
