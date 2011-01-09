package org.zoodb.test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class Test_050_ObjectCreation {

	private static final String DB_NAME = "TestDb";
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(DB_NAME, TestClass.class);
	}

	@After
	public void after() {
		TestTools.closePM();
	}
	
	@Test
	public void testObjectCreation() {
		System.out.println("Testing Objects");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		//return null for transient (JDO 2.2, p45)
		Object oidT = pm.getObjectId(tc);
		assertNull(oidT);
		
		pm.makePersistent(tc);
		Object oidP = pm.getObjectId(tc);
		assertFalse(oidT == oidP);
		assertFalse(oidP.equals(oidT));
		
		Object oidP2 = pm.getObjectId(tc);
		assertTrue(oidP.equals(oidP2));

		//TODO JDO 2.2 (javadoc) says, only copies should be returned.
		//If we return long/Long, then OIDs are immutable and don't need
		//to be copied, so object identity for OIDs should be allowed(?).
		//assertFalse(oidP == oidP2);
		if (oidP == oidP2) {
			System.err.println("Warning: OID is not cloned.");
		}
		
		assertTrue(JDOHelper.isNew(tc));
		assertTrue(JDOHelper.isDirty(tc)); //JDO 2.2 12.6.7 makePersistent()
		//persistent before commit!
		assertTrue(JDOHelper.isPersistent(tc)); //JDO 2.2 12.6.7 makePersistent()
		
		
		
		//TODO cheat, return longs? They are can be identical, but they
		//are not modifyable!
	
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	

	@Test
	public void testOidUsage() {
		System.out.println("Testing OID usage - TODO");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setData(131, true, 'x', (byte)12, (short)-2, 132132, "tach!", new byte[]{1,1,2,3,5,8});
		pm.makePersistent(tc);
		Object oidP = pm.getObjectId(tc);
		pm.currentTransaction().commit();
		
		assertFalse(JDOHelper.isNew(tc));
		assertFalse(JDOHelper.isDirty(tc));
		//persistent after commit!
		assertTrue(JDOHelper.isPersistent(tc));
		
		TestTools.closePM();
		tc = null;
		
		
		//now load the object
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		TestClass tc2 = (TestClass) pm.getObjectById(oidP);
		Object oidP2 = pm.getObjectId(tc2);
		assertTrue(oidP.equals(oidP2));

		//check some flags
		assertFalse(JDOHelper.isNew(tc2));
		assertFalse(JDOHelper.isDirty(tc2));
		assertTrue(JDOHelper.isPersistent(tc2));
		
		tc2.checkData(131, true, 'x', (byte)12, (short)-2, 132132, "tach!", new byte[]{1,1,2,3,5,8});
		
		//make dirty
		JDOHelper.makeDirty(tc2, "_long");
		assertTrue(JDOHelper.isDirty(tc2));
		pm.currentTransaction().commit();
		//is test valid outside tx?
		assertFalse(JDOHelper.isDirty(tc2));
		
		//delete
		pm.currentTransaction().begin();
		assertFalse(JDOHelper.isDeleted(tc2));
		pm.deletePersistent(tc2);
		assertTrue(JDOHelper.isDeleted(tc2));
		pm.currentTransaction().commit();


		//check deleted in session
		pm.currentTransaction().begin();
		try {
			pm.getObjectById(oidP);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}

		pm.currentTransaction().rollback();
		TestTools.closePM();
		pm = null;

		//check deleted next session
		PersistenceManager pm2 = TestTools.openPM();
		pm2.currentTransaction().begin();
		try {
			pm2.getObjectById(oidP);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good
		}
		pm2.currentTransaction().rollback();
		pm2.close();
		TestTools.closePM();
	}
		
	
	@Test
	public void testObjectTree() {
		System.out.println("Testing Object Tree");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		TestClass tc1 = new TestClass();
		TestClass tc2 = new TestClass();
		tc.setData(131, true, 'x', (byte)12, (short)-2, 132132, "tach!", new byte[]{1,1,2,3,5,8});
		tc1.setData(111, true, 'y', (byte)13, (short)-23, 1132132, "Tach!", new byte[]{1,1,2,3,5});
		tc2.setData(222, true, 'z', (byte)14, (short)-234, 11132132, "TACH!", new byte[]{1,1,2,3});
		tc.setRef1(tc1);
		tc.setRef2(tc2);
		tc1.setRef1(tc);
		tc2.setRef1(tc);
		tc1.setRef2(tc2);
		tc2.setRef2(tc1);
		pm.makePersistent(tc);
		Object oidP = pm.getObjectId(tc);
		pm.currentTransaction().commit();
		Object oidP1 = pm.getObjectId(tc1);
		Object oidP2 = pm.getObjectId(tc2);
		
		TestTools.closePM();
		pm = null;
		tc = null;
		
		
		//now load the object
		pm = TestTools.openPM();
		pm.currentTransaction().begin();

		tc = (TestClass) pm.getObjectById(oidP);
		pm.getObjectById(oidP1);
		pm.getObjectById(oidP2);
		tc1 = (TestClass) tc.getRef1();
		tc2 = tc.getRef2();
		
		//check OIDs
		assertEquals(oidP1, pm.getObjectId(tc1));
		assertEquals(oidP2, pm.getObjectId(tc2));
		
		//check data
		tc.checkData(131, true, 'x', (byte)12, (short)-2, 132132, "tach!", new byte[]{1,1,2,3,5,8});
		tc1.checkData(111, true, 'y', (byte)13, (short)-23, 1132132, "Tach!", new byte[]{1,1,2,3,5});
		tc2.checkData(222, true, 'z', (byte)14, (short)-234, 11132132, "TACH!", new byte[]{1,1,2,3});
		
		//check refs
		assertEquals(tc1, tc.getRef1());
		assertEquals(tc2, tc.getRef2());
		assertEquals(tc, tc1.getRef1());
		assertEquals(tc, tc2.getRef1());
		assertEquals(tc2, tc1.getRef2());
		assertEquals(tc1, tc2.getRef2());
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	@Test
	public void testLargerOidIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		start("creating objects");
		
		final int nObj = 100000;
		for (int i = 0; i < nObj; i++) {
			TestClass pc = new TestClass();
			pc.setInt(i+1);
			pc.setLong(35);
			pm.makePersistent(pc);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();

		stop("creating objects");
		
		//now read it all
		pm = TestTools.openPM();

		start("reading objects");
		
		pm.currentTransaction().begin();
		
		Extent<TestClass> ex = pm.getExtent(TestClass.class);
		int n = 0;
		for (TestClass pc: ex) {
			if (pc.getLong() == 35) { 
				assertTrue("Error: " + pc.getInt(), pc.getInt() > 0 && pc.getInt() < nObj+2);
				n++;
			}
		}
		assertTrue("Objects found: " + n, n==nObj);
		
		stop("reading objects");

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		start("deleting objects");
		
        Extent<TestClass> extent = pm.getExtent(TestClass.class, false);
        Iterator<TestClass> it = extent.iterator();
        int nDel = 0;
        while(it.hasNext()){
            pm.deletePersistent(it.next());
            nDel++;
        }
        assertTrue("Found: " + nDel + " expected" + nObj, nDel >= nObj);
        extent.closeAll();
		
		pm.currentTransaction().commit();

		stop("deleting objects");

		pm.currentTransaction().begin();

        extent = pm.getExtent(TestClass.class, false);
        Iterator<TestClass> it2 = extent.iterator();
        assertFalse(it2.hasNext());

		
		pm.currentTransaction().commit();

		TestTools.closePM();
	}
	
	
	@Test
	public void testLargerOidIndexWithDbClose() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		start("creating objects");
		
		final int nObj = 100000;
		for (int i = 0; i < nObj; i++) {
			TestClass pc = new TestClass();
			pc.setInt(i+1);
			pm.makePersistent(pc);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();

		stop("creating objects");
		
		//now read it all
		pm = TestTools.openPM();

		start("reading objects");
		
		pm.currentTransaction().begin();
		
		Extent<TestClass> ex = pm.getExtent(TestClass.class);
		int n = 0;
		for (TestClass pc: ex) {
			assertTrue("pc,getInt()=" + pc.getInt(), pc.getInt() > 0 && pc.getInt() < nObj+2);
			n++;
		}
		assertTrue("Objects found: " + n, n>=nObj);
		
		stop("reading objects");

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		start("deleting objects");
		
        Extent<TestClass> extent = pm.getExtent(TestClass.class, false);
        Iterator<TestClass> it = extent.iterator();
        int nDel = 0;
        while(it.hasNext()){
            pm.deletePersistent(it.next());
            nDel++;
        }
        assertTrue("Found: " + nDel + " expected" + nObj, nDel >= nObj);
        extent.closeAll();
		
		pm.currentTransaction().commit();

		stop("deleting objects");

		pm.currentTransaction().begin();

        extent = pm.getExtent(TestClass.class, false);
        Iterator<TestClass> it2 = extent.iterator();
        assertFalse(it2.hasNext());

		
		pm.currentTransaction().commit();

		TestTools.closePM();
	}
	
	
	private long _time;
	private void start(String msg) {
		_time = System.currentTimeMillis();
	}
	private void stop(String msg) {
		long t = System.currentTimeMillis() - _time;
		double td = t/1000.0;
		System.out.println(msg + ": " + td);
	}

	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
	
}
