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

import static org.junit.Assert.*;

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.api.ZooHandle;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.testutil.TestTools;

public class Test_034_SchemaEvolution {
	
	@AfterClass
	public static void tearDown() {
	    TestTools.closePM();
	}

	@Before
	public void before() {
		TestTools.createDb();
	}
	
	@After
	public void after() {
		TestTools.closePM();
		TestTools.removeDb();
	}

	
	@Test
	public void testSimpleEvolution() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema.defineClass(pm, TestClassTiny.class);
		TestClassTiny t1 = new TestClassTiny(1, 3);
		TestClassTiny t2 = new TestClassTiny(4, 5);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		Object oid1 = pm.getObjectId(t1);
		Object oid2 = pm.getObjectId(t2);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		pm = TestTools.openPM();
		
		pm.currentTransaction().begin();
		
		ZooClass s1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		s1.rename(TestClassSmall.class.getName());
//		private int myInt;
//		private long myLong;
//		private String myString;
//		private int[] myInts;
//		private Object refO;
//		private TestClassTiny refP;

		s1.getField("_int").remove();
		s1.defineField("myInt", Integer.TYPE);
		s1.getField("_long").rename("myLong");
		s1.defineField("myString", String.class);
		s1.defineField("myInts", Integer[].class);
		s1.defineField("refO", Object.class);
		s1.defineField("refP", TestClassTiny.class);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClassSmall ts1 = (TestClassSmall) pm.getObjectById(oid1);
		TestClassSmall ts2 = (TestClassSmall) pm.getObjectById(oid2);

		assertEquals(0, ts1.getMyInt());
		assertEquals(3, ts1.getMyLong());
		assertEquals(0, ts2.getMyInt());
		assertEquals(5, ts2.getMyLong());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testDataMigration() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema.defineClass(pm, TestClassTiny.class);
		TestClassTiny t1 = new TestClassTiny(1, 3);
		TestClassTiny t2 = new TestClassTiny(4, 5);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		Object oid1 = pm.getObjectId(t1);
		Object oid2 = pm.getObjectId(t2);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		pm = TestTools.openPM();
		
		pm.currentTransaction().begin();
		
		ZooClass s1 = ZooSchema.locateClass(pm, TestClassTiny.class.getName());
		s1.rename(TestClassSmall.class.getName());
//		private int myInt;
//		private long myLong;
//		private String myString;
//		private int[] myInts;
//		private Object refO;
//		private TestClassTiny refP;

		//additive changes
		s1.defineField("myInt", Integer.TYPE);
		s1.getField("_long").rename("myLong");
		s1.defineField("myString", String.class);
		s1.defineField("myInts", Integer[].class);
		s1.defineField("refO", Object.class);
		s1.defineField("refP", TestClassTiny.class);
		
		Iterator<ZooHandle> it = s1.getHandleIterator(false);
		int n = 0;
		while (it.hasNext()) {
			//migrate data
			ZooField f1 = s1.getField("_int");
			ZooField f2a = s1.getField("myInt"); //new
			ZooField f2b = s1.getField("myLong");  //renamed
			ZooField f2c = s1.getField("myString");  //new String
			
			ZooHandle hdl = it.next();
			//TODO pass in field instead?!?!
			int i = (Integer) f1.getValue(hdl);
			f2a.setValue(hdl, i+1);
			f2b.setValue(hdl, (long)i+2);
			f2c.setValue(hdl, String.valueOf(i+3));
			
	        assertEquals(i+1, (int)(Integer) f2a.getValue(hdl));
	        assertEquals(i+2, (long)(Long) f2b.getValue(hdl));
	        assertEquals(String.valueOf(i+=3), f2c.getValue(hdl));
	        
			//batch processing 
			pm.currentTransaction().commit();
			pm.currentTransaction().begin();
			n++;
		}
		assertEquals(2, n);
		//destructive changes
		s1.getField("_int").remove();
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClassSmall ts1 = (TestClassSmall) pm.getObjectById(oid1);
		TestClassSmall ts2 = (TestClassSmall) pm.getObjectById(oid2);

		assertEquals(2, ts1.getMyInt());
        assertEquals(3, ts1.getMyLong());
        assertEquals("4", ts1.getMyString());
		assertEquals(5, ts2.getMyInt());
		assertEquals(6, ts2.getMyLong());
        assertEquals("7", ts2.getMyString());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
    @Test
    public void testSetOid() {
    	TestTools.defineSchema(TestClass.class);
    	TestTools.defineIndex(TestClass.class, "_long", false);
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass cls = ZooSchema.locateClass(pm, TestClass.class);
		TestClass tc = new TestClass();
		tc.setInt(1);
		tc.setLong(2);
		pm.makePersistent(tc);
		Object oidc0 =  pm.getObjectId(tc);
		ZooHandle hdlC1 = cls.newInstance();
		hdlC1.setValue("_int", 21);
		hdlC1.setValue("_long", 22L);
		Object oidc1 = hdlC1.getOid();
		Object oidc2 = 10 + (Long)oidc1;
		ZooHandle hdlC2 = cls.newInstance((Long) oidc2);
		hdlC2.setValue("_int", 31);
		hdlC2.setValue("_long", 32L);
		assertEquals(oidc2, hdlC2.getOid());

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//create objects for testing setOId on new-objects (TODO, use only 1 set of object and
		//repeat tests?
//		TestClass t = new TestClass();
//		t.setInt(41);
//		t.setLong(42);
//		pm.makePersistent(t);
//		Object oid0 =  pm.getObjectId(t);
		ZooHandle hdl1 = cls.newInstance();
		hdl1.setValue("_int", 51);
		hdl1.setValue("_long", 52L);
		Object oid1 = hdl1.getOid();
		Object oid2 = 10 + (Long)oid1;
		ZooHandle hdl2 = cls.newInstance((Long) oid2);
		hdl2.setValue("_int", 61);
		hdl2.setValue("_long", 62L);
		assertEquals(oid2, hdl2.getOid());

		//Now set OIDs
		
		ZooHandle hdlC0 = ZooSchema.getHandle(pm, (Long) oidc0);
		
		//TODO
		//test rollback
		//test swapping OIDs (also between PC and GO
		
		
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
        fail(); //TODO
    }

    /**
     * Check that non-evolved objects are still returned when a query matches the default value
     * of a recently added field.
     */
    @Test
    public void testQueryNonEvolvedObjectsOnDefaultValueWithFieldIndex() {
        fail(); //TODO
    }

    @Test
    public void testCleanUpOfPreviousVersions() {
        //query on class oid?
        //db size?
        //iterate old pos-index?
        //iterate value-index?
        fail(); //TODO
    }

    /**
     * Test that an object that is evolved becomes rewritten. How can we check this?
     * a) Without modification, the database should still write pages (change size?)
     * b) The object should be dirty? (Actually, not necessarily, the user should not notice)
     * c) Instance callbacks? The same as b)
     * 
     * --> Enable/disable isEvolved in DataDeSerializer 
     */
    @Test
    public void testEvolutionReWrite() {
        fail(); //TODO
    }

    @Test
    public void testArrayAttributes() {
        fail(); //TODO
    }


    /**
     * Ensure that evolution does not occur if set to 'manual'. 
     */
    @Test
    public void testFailIfNotAutomatic() {
    	fail(); //TODO
    }
    
    @Test
    public void testInctanceCount() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass c1 = ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooClass c2 = ZooSchema.defineClass(pm, TestClassTiny2.class);
		assertEquals(0, c1.instanceCount(false));
		assertEquals(0, c1.instanceCount(true));
		assertEquals(0, c2.instanceCount(true));
		
		TestClassTiny t1 = new TestClassTiny(1, 3);
		TestClassTiny t2 = new TestClassTiny(4, 5);
		assertEquals(0, c1.instanceCount(false));
		assertEquals(0, c1.instanceCount(true));
		assertEquals(0, c2.instanceCount(true));
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		assertEquals(0, c1.instanceCount(false));
		assertEquals(0, c1.instanceCount(true));
		assertEquals(0, c2.instanceCount(true));

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		c1 = ZooSchema.locateClass(pm, TestClassTiny.class);
		//TODO Implement?!?!?
		fail();
		assertEquals(1, c1.instanceCount(false));
		assertEquals(2, c1.instanceCount(true));
		assertEquals(1, c2.instanceCount(true));

		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		c1 = ZooSchema.locateClass(pm, TestClassTiny.class);
		assertEquals(1, c1.instanceCount(false));
		assertEquals(2, c1.instanceCount(true));
		assertEquals(1, c2.instanceCount(true));

		c1.remove();
		assertEquals(1, c1.instanceCount(false));
		assertEquals(2, c1.instanceCount(true));
		assertEquals(1, c2.instanceCount(true));

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		assertEquals(0, c1.instanceCount(false));
		assertEquals(0, c1.instanceCount(true));
		assertEquals(0, c2.instanceCount(true));

		pm.currentTransaction().commit();
		TestTools.closePM();
    }
    
    @Test
    public void testFailSetValue() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass c1 = ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooClass c2 = ZooSchema.defineClass(pm, TestClassTiny2.class);
		ZooHandle h1 = c1.newInstance();
		ZooHandle h2 = c2.newInstance();
		ZooField f1 = c1.getField("_long");
		ZooField f2 = c2.getField("i2");
		
		//this should work because f1 is in the super-class of h2.
		f1.setValue(h2, 3L);
		
		try {
			f2.setValue(h1, 5);
			fail();
		} catch (IllegalArgumentException e) {
			//ok
		}
		
		try {
			f1.setValue(h1, 3);
			fail();
		} catch (IllegalArgumentException e) {
			//ok, because the parameter should be a Long!
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
    }

    @Test
    public void testNewInstance() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass c1 = ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooClass c2 = ZooSchema.defineClass(pm, TestClassTiny2.class);
		ZooHandle h1 = c1.newInstance();
		ZooHandle h2 = c2.newInstance();
		ZooHandle hRem = c2.newInstance();
		ZooField f1 = c1.getField("_long");
		ZooField f2 = c2.getField("i2");
		f1.setValue(h1, 3L);
		f2.setValue(h2, 5);
		Object oid1 = h1.getOid();
		Object oid2 = h2.getOid();
		Object oidRem = hRem.getOid();
		//remove non-committed object
		hRem.remove();
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClassTiny t1 = (TestClassTiny) pm.getObjectById(oid1);
		TestClassTiny2 t2 = (TestClassTiny2) pm.getObjectById(oid2);
		try {
			pm.getObjectById(oidRem);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good!
		}
		assertEquals(3, t1.getLong());
		assertEquals(5, t2.getInt2());
		
		pm.currentTransaction().commit();
		TestTools.closePM();
    }

    @Test
    public void testHandleValidity() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass c1 = ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooClass c2 = ZooSchema.defineClass(pm, TestClassTiny2.class);
		ZooField f1 = c1.getField("_int");
		ZooHandle h1 = c1.newInstance();
		ZooHandle h2 = c2.newInstance();
		ZooHandle hRem = c2.newInstance();
		//remove non-committed object
		hRem.remove();

		try {
			f1.getValue(hRem);
			fail();
		} catch (IllegalStateException e) {
			//object is removed
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();

		try {
			h1.getAttrLong("i2");
			fail();
		} catch (IllegalStateException e) {
			//outside tx
		}

		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		c1 = ZooSchema.locateClass(pm, TestClassTiny.class);
		f1 = c1.getField("_int");
		
		try {
			h2.getAttrLong("i2");
			fail();
		} catch (IllegalStateException e) {
			//wrong session
		}
		
		try {
			f1.getValue(h2);
			fail();
		} catch (IllegalStateException e) {
			//object is removed
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
    }

    @Test
    public void testHandleRemove() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooSchema.defineClass(pm, TestClassTiny2.class);
		TestClassTiny t1 = new TestClassTiny();
		TestClassTiny2 t2 = new TestClassTiny2();
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		Object oid1 = pm.getObjectId(t1);
		Object oid2 = pm.getObjectId(t2);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooHandle h1 = ZooSchema.getHandle(pm, (Long)oid1); 
		ZooHandle h2 = ZooSchema.getHandle(pm, (Long)oid2);
		h1.remove();
		h2.remove();
		
		//try delete again
		try {
			h1.remove();
			fail();
		} catch (Exception e) {
			//object is already deleted
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		try {
			pm.getObjectById(oid1);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}
		try {
			pm.getObjectById(oid2);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}
		
		Extent<?> ext = pm.getExtent(TestClassTiny.class);
		assertFalse(ext.iterator().hasNext());
		ext.closeAll();
		
		pm.currentTransaction().commit();
		TestTools.closePM();
    }
    
    @Test
    public void testHandleGetJavaObject() {
    	TestTools.defineSchema(TestClassTiny.class);
    	PersistenceManager pm = TestTools.openPM();
    	pm.currentTransaction().begin();

		TestClassTiny t1 = new TestClassTiny(1, 3);
		TestClassTiny t2 = new TestClassTiny(4, 5);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		Object oid1 = pm.getObjectId(t1);
		Object oid2 = pm.getObjectId(t2);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		ZooHandle h1 = ZooSchema.getHandle(pm, (Long)oid1);
		ZooHandle h2 = ZooSchema.getHandle(pm, (Long)oid2);

		assertTrue(t1 == h1.getJavaObject());

		h1.remove();
		try {
			h1.getJavaObject();
			fail();
		} catch (IllegalStateException e) {
			//good, has been deleted
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		try {
			h2.getJavaObject();
			fail();
		} catch (IllegalStateException e) {
			//good, outside tx
		}
    }
    
    @Test
    public void testHandleGetType() {
    	TestTools.defineSchema(TestClassTiny.class);
    	PersistenceManager pm = TestTools.openPM();
    	pm.currentTransaction().begin();

    	ZooClass s = ZooSchema.locateClass(pm, TestClassTiny.class);
    	
		TestClassTiny t1 = new TestClassTiny(1, 3);
		TestClassTiny t2 = new TestClassTiny(4, 5);
		pm.makePersistent(t1);
		pm.makePersistent(t2);
		Object oid1 = pm.getObjectId(t1);
		Object oid2 = pm.getObjectId(t2);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		ZooHandle h1 = ZooSchema.getHandle(pm, (Long)oid1);
		ZooHandle h2 = ZooSchema.getHandle(pm, (Long)oid2);

		assertTrue(s == h1.getType());

		h1.remove();
		try {
			h1.getType();
			fail();
		} catch (IllegalStateException e) {
			//good, has been deleted
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		try {
			h2.getType();
			fail();
		} catch (IllegalStateException e) {
			//good, outside tx
		}
    }
    

	@Test
	public void testMakePersistent() {
		getSuperClass(true);
	}

	@Test
	public void testMakePersistentNoCommit() {
		getSuperClass(false);
	}
	
	private void getSuperClass(boolean commit) {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass clsPC = ZooSchema.locateClass(pm, PersistenceCapableImpl.class.getName());
		ZooClass c1 = ZooSchema.defineEmptyClass(pm, TestClassTiny.class.getName(), clsPC);
		c1.defineField("_int",  Integer.TYPE);
		c1.defineField("_long",  Long.TYPE);
		
		if (commit) {
			pm.currentTransaction().commit();
			pm.currentTransaction().begin();
		}

		TestClassTiny t1 = new TestClassTiny();
		pm.makePersistent(t1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	@Test
	public void testFailClassIncompatibility() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass clsPC = ZooSchema.locateClass(pm, PersistenceCapableImpl.class.getName());
		ZooClass c1 = ZooSchema.defineEmptyClass(pm, TestClassTiny.class.getName(), clsPC);
		c1.defineField("_int",  Integer.TYPE);

		TestClassTiny t1 = new TestClassTiny();
		try {
			pm.makePersistent(t1);
			fail();
		} catch (JDOUserException e) {
			//field missing
		}

		c1.defineField("_long",  Integer.TYPE);

		try {
			pm.makePersistent(t1);
			fail();
		} catch (JDOUserException e) {
			//field has wrong type
		}

		c1.getField("_long").remove();
		c1.defineField("_long",  Long.TYPE);
		
		//now it should work
		pm.makePersistent(t1);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	@Test
	public void testFailDoubleDeclare() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass clsPC = ZooSchema.locateClass(pm, PersistenceCapableImpl.class.getName());
		ZooClass c1 = ZooSchema.defineEmptyClass(pm, TestClassTiny.class.getName(), clsPC);
		c1.defineField("_int",  Integer.TYPE);
		c1.defineField("_long",  Long.TYPE);

		//same tx
		try {
			ZooSchema.defineEmptyClass(pm, TestClassTiny.class.getName(), clsPC);
			fail();
		} catch (IllegalArgumentException e) {
			//already defined
		}

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//new tx
		try {
			clsPC = ZooSchema.locateClass(pm, PersistenceCapableImpl.class.getName());
			ZooSchema.defineEmptyClass(pm, TestClassTiny.class.getName(), clsPC);
			fail();
		} catch (IllegalArgumentException e) {
			//already defined
		}

		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//new session
		try {
			clsPC = ZooSchema.locateClass(pm, PersistenceCapableImpl.class.getName());
			ZooSchema.defineEmptyClass(pm, TestClassTiny.class.getName(), clsPC);
			fail();
		} catch (IllegalArgumentException e) {
			//already defined
		}

		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	@Test
	public void testLocate() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass clsPC = ZooSchema.locateClass(pm, PersistenceCapableImpl.class.getName());
		ZooClass c1 = ZooSchema.defineEmptyClass(pm, TestClassTiny.class.getName(), clsPC);
		c1.defineField("_int",  Integer.TYPE);
		c1.defineField("_long",  Long.TYPE);

		//same tx
		assertNotNull(ZooSchema.locateClass(pm, TestClassTiny.class));
		assertNotNull(ZooSchema.locateClass(pm, TestClassTiny.class.getName()));

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		//new tx
		assertNotNull(ZooSchema.locateClass(pm, TestClassTiny.class));
		assertNotNull(ZooSchema.locateClass(pm, TestClassTiny.class.getName()));

		pm.currentTransaction().commit();
		TestTools.closePM();
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//new session
		assertNotNull(ZooSchema.locateClass(pm, TestClassTiny.class));
		assertNotNull(ZooSchema.locateClass(pm, TestClassTiny.class.getName()));

		pm.currentTransaction().commit();
		TestTools.closePM();
	}

}
