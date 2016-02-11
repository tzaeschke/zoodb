
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
 
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
 
import java.util.Collection;
 
import javax.jdo.Constants;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
 
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooHandle;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.testutil.TestTools;
 
public class Test_150_TxNonTxRead {
 
    private ZooJdoProperties props;
    
    @BeforeClass
    public static void setUp() {
        TestTools.createDb();
        TestTools.defineSchema(TestClass.class);
    }
 
    @Before
    public void before() {
        TestTools.dropInstances(TestClass.class);
        props = TestTools.getProps();
        props.setNontransactionalRead(true);
    }
    
    @After
    public void afterTest() {
        TestTools.closePM();
    }
    
    @AfterClass
    public static void tearDown() {
        TestTools.removeDb();
    }
 
    @Test
    public void testPropertyPropagation() {
        ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
        PersistenceManagerFactory pmf;
        PersistenceManager pm;
 
        //all false, vary pm
        assertEquals("false", props.getProperty(Constants.PROPERTY_NONTRANSACTIONAL_READ));
        pmf = JDOHelper.getPersistenceManagerFactory(props);
        assertFalse(pmf.getNontransactionalRead());
        pm = pmf.getPersistenceManager();
        assertFalse(pm.currentTransaction().getNontransactionalRead());
        pm.currentTransaction().setNontransactionalRead(true);
        assertTrue(pm.currentTransaction().getNontransactionalRead());
        pm.currentTransaction().setNontransactionalRead(false);
        assertFalse(pm.currentTransaction().getNontransactionalRead());
        pm.close();
        
        //pmf true, vary pm
        pmf = JDOHelper.getPersistenceManagerFactory(props);
        pmf.setNontransactionalRead(true);
        assertTrue(pmf.getNontransactionalRead());
        pm = pmf.getPersistenceManager();
        assertTrue(pm.currentTransaction().getNontransactionalRead());
        pm.currentTransaction().setNontransactionalRead(false);
        assertFalse(pm.currentTransaction().getNontransactionalRead());
        pm.currentTransaction().setNontransactionalRead(true);
        assertTrue(pm.currentTransaction().getNontransactionalRead());
        pm.close();
 
        //props true
        props.setNontransactionalRead(true);
        assertEquals("true", props.getProperty(Constants.PROPERTY_NONTRANSACTIONAL_READ));
        pmf = JDOHelper.getPersistenceManagerFactory(props);
        assertTrue(pmf.getNontransactionalRead());
    }
 
 
    @Test
    public void testNoTransactionalSchema() {
        PersistenceManager pm = TestTools.openPM(props);
        
        //non-tx read
        ZooSchema s = ZooJdoHelper.schema(pm);
 
        try {
            s.addClass(TestClassTiny.class);
            fail();
        } catch (IllegalStateException e) {
            //write should fail
        }
 
        //update
        pm.currentTransaction().begin();
        s.addClass(TestClassTiny.class);
        pm.currentTransaction().commit();
        
        //non-tx read
        assertNotNull(s.getClass(TestClassTiny.class));
 
        pm.currentTransaction().begin();
        try {
            s.addClass(TestClassTiny.class);
            fail();
        } catch (JDOUserException e) {
            //good, is already defined
        }
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }
    
    @Test
    public void testNonTransactionalGenericObjects() {
    	fail();//TODO This breaks everything else...
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        ZooSchema s = ZooJdoHelper.schema(pm);
        ZooClass c = s.getClass(TestClass.class);
        c.addField("test1", Integer.TYPE);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
 
        ZooHandle h = c.newInstance();
        Object oid = h.getOid();
        
        pm.currentTransaction().commit();
 
        //TODO fix other tests, this is not a GO read
        
        //non-tx read
        //assertNotNull(pm.getObjectById(oid));
        ZooHandle hdl = s.getHandle(oid);
        assertEquals(0, hdl.getAttrInt("test1"));
        
        TestTools.closePM();
    }
    
    @Test
    public void testStateTransitions() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();
        
        TestClass tc1 = new TestClass();
        TestClass tc1b = new TestClass();
        pm.makePersistent(tc1);
        pm.makePersistent(tc1b);
        tc1.setInt(5);
        tc1.setRef2(tc1b);
        tc1b.setInt(6);
 
        pm.currentTransaction().commit();
        
        assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc1));
        assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc1b));
        
        assertEquals(6, tc1.getRef2().getInt());
        
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1));
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1b));
        
        try {
            tc1.setRef2(null);
            fail();
        } catch (JDOUserException e) {
            //non-tx write not allowed
        }
 
        try {
            tc1b.setInt(60);
            fail();
        } catch (JDOUserException e) {
            //non-tx write not allowed
        }
 
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1));
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1b));
        
        pm.refresh(tc1);
        pm.refresh(tc1b);
 
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1));
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1b));
    
        TestTools.closePM();
    }
 
    @Test
    public void testReadNavigation() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();
        
        TestClass tc1 = new TestClass();
        TestClass tc1b = new TestClass();
        pm.makePersistent(tc1);
        pm.makePersistent(tc1b);
        tc1.setInt(5);
        tc1.setRef2(tc1b);
        tc1b.setInt(6);
        
        pm.currentTransaction().commit();
 
        //test navigation
        assertEquals(5, tc1.getInt());
        assertEquals(6, tc1.getRef2().getInt());
        
        TestTools.closePM();
    }
    
    @Test
    public void testReadByOid() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();
        
        //create data
        TestClass tc1 = new TestClass();
        TestClass tc1b = new TestClass();
        pm.makePersistent(tc1);
        pm.makePersistent(tc1b);
        tc1.setInt(5);
        tc1.setRef2(tc1b);
        tc1b.setInt(6);
        Object oid1 = pm.getObjectId(tc1);
        pm.currentTransaction().commit();
 
        //test oid+navigation
        tc1 = (TestClass) pm.getObjectById(oid1);
        assertEquals(5, tc1.getInt());
        assertEquals(6, tc1.getRef2().getInt());
        
        TestTools.closePM();
        
        //test with new TX
        pm = TestTools.openPM(props);
 
        //test oid+navigation
        tc1 = (TestClass) pm.getObjectById(oid1);
        assertEquals(5, tc1.getInt());
        assertEquals(6, tc1.getRef2().getInt());
        
        TestTools.closePM();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testReadQuery() {
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();
 
        //create data
        TestClass tc1 = new TestClass();
        TestClass tc1b = new TestClass();
        pm.makePersistent(tc1);
        pm.makePersistent(tc1b);
        tc1.setInt(5);
        tc1.setRef2(tc1b);
        tc1b.setInt(6);
        pm.currentTransaction().commit();
 
        //test navigation
        Query q = pm.newQuery(TestClass.class, "_int == 5");
        Collection<TestClass> c = (Collection<TestClass>) q.execute();
        tc1 = c.iterator().next();
        assertEquals(5, tc1.getInt());
        assertEquals(6, tc1.getRef2().getInt());
        
        TestTools.closePM();
        
        //test with new TX
        pm = TestTools.openPM(props);
 
        //test oid+navigation
        q = pm.newQuery(TestClass.class, "_int == 5");
        c = (Collection<TestClass>) q.execute();
        tc1 = c.iterator().next();
        assertEquals(5, tc1.getInt());
        assertEquals(6, tc1.getRef2().getInt());
        
        TestTools.closePM();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testReadQueryNoSchema() {
        PersistenceManager pm = TestTools.openPM(props);
 
        //test
        Query q = pm.newQuery(TestClassSmallA.class, "myInt == 5");
        Collection<TestClass> c = (Collection<TestClass>) q.execute();
        assertFalse( c.iterator().hasNext() );
        TestTools.closePM();
    }
    
    @Test
    public void testMultiSession() {
        fail();
        //TODO test that multi-session is not possible with non-tx-read enabled
    }
    
    @Test
    public void testNpeAfterSchemaChange() {
    	fail();
        //TODO move to schema test suite
        ZooJdoProperties props = TestTools.getProps();
        props.setNontransactionalRead(true);
        PersistenceManager pm = TestTools.openPM(props);
        pm.currentTransaction().begin();
        
        ZooSchema s = ZooJdoHelper.schema(pm);
        ZooClass c = s.getClass(TestClass.class);
        c.addField("test1", Integer.TYPE);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
 
        ZooHandle h = c.newInstance();
        Object oid = h.getOid();
        
        pm.currentTransaction().commit();
 
        //TODO fix other tests, this is not a GO read
        
        //non-tx read
        //assertNotNull(pm.getObjectById(oid));
        ZooHandle hdl = s.getHandle(oid);
        assertEquals(0, hdl.getAttrInt("test1"));
        
        TestTools.closePM();
 
        TestTools.openPM(props);
        pm.currentTransaction().begin();
        
        TestClass tc1 = new TestClass();
        TestClass tc1b = new TestClass();
        pm.makePersistent(tc1);
        pm.makePersistent(tc1b);
        tc1.setInt(5);
        tc1.setRef2(tc1b);
        tc1b.setInt(6);
 
        pm.currentTransaction().commit();
        
        assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc1));
        assertEquals(ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL, JDOHelper.getObjectState(tc1b));
        
        assertEquals(6, tc1.getRef2().getInt());
        
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1));
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1b));
        
        try {
            tc1.setRef2(null);
            fail();
        } finally {
            //non-tx write not allowed
        }
 
        try {
            tc1b.setInt(60);
            fail();
        } finally {
            //non-tx write not allowed
        }
 
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1));
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1b));
        
        pm.refresh(tc1);
        pm.refresh(tc1b);
 
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1));
        assertEquals(ObjectState.PERSISTENT_CLEAN, JDOHelper.getObjectState(tc1b));
    
        TestTools.closePM();
    }
 
    
}
 
