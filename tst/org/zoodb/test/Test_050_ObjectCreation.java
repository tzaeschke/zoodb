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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooConfig;
import org.zoodb.test.testutil.TestTools;

public class Test_050_ObjectCreation {

	@BeforeClass
	public static void setUp() {
		TestTools.removeDb();
		//Config.setFileManager(Config.FILE_MGR_IN_MEMORY);
		//Config.setFileProcessor(Config.FILE_PAF_BB_MAPPED_PAGE);
		//Config.setFilePageSize(Config.FILE_PAGE_SIZE_DEFAULT * 4);
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class, TestClassTiny.class);
	}

	@After
	public void after() {
		TestTools.closePM();
	}
	
	@Test
	public void testObjectCreation() {
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

		//JDO 2.2 (javadoc) says, only copies should be returned.
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
	
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testOidUsage() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		tc.setData(131, true, 'x', (byte)12, (short)-2, 132132, "tach!", new byte[]{1,1,2,3,5,8},
				-1.1f, 35);
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
		
		tc2.checkData(131, true, 'x', (byte)12, (short)-2, 132132, "tach!", new byte[]{1,1,2,3,5,8},
				-1.1f, 35);
		
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
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		TestClass tc = new TestClass();
		TestClass tc1 = new TestClass();
		TestClass tc2 = new TestClass();
		tc.setData(131, true, 'x', (byte)12, (short)-2, 132132, "tach!", new byte[]{1,1,2,3,5,8},
				-1.13f, 35.3);
		tc1.setData(111, true, 'y', (byte)13, (short)-23, 1132132, "Tach!", new byte[]{1,1,2,3,5},
				-1.14f, 35.4);
		tc2.setData(222, true, 'z', (byte)14, (short)-234, 11132132, "TACH!", new byte[]{1,1,2,3},
				-1.15f, 35.5);
		tc.setRef1(tc1);
		tc.setRef2(tc2);
		tc1.setRef1(tc);
		tc2.setRef1(tc);
		tc1.setRef2(tc2);
		tc2.setRef2(tc1);
		pm.makePersistent(tc);
		Object oidP = pm.getObjectId(tc);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		Object oidP1 = pm.getObjectId(tc1);
		Object oidP2 = pm.getObjectId(tc2);
		
		pm.currentTransaction().commit();
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
		tc.checkData(131, true, 'x', (byte)12, (short)-2, 132132, "tach!", new byte[]{1,1,2,3,5,8},
				-1.13f, 35.3);
		tc1.checkData(111, true, 'y', (byte)13, (short)-23, 1132132, "Tach!", new byte[]{1,1,2,3,5},
				-1.14f, 35.4);
		tc2.checkData(222, true, 'z', (byte)14, (short)-234, 11132132, "TACH!", new byte[]{1,1,2,3},
				-1.15f, 35.5);
		
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

		final int nObj = 1000000;
		for (int i = 0; i < nObj; i++) {
			TestClass pc = new TestClass();
			pc.setInt(i+1);
			pc.setLong(35);
			pm.makePersistent(pc);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();
		
		//now read it all
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		Extent<TestClass> ex = pm.getExtent(TestClass.class);
		int n = 0;
		for (TestClass pc: ex) {
			if (pc.getLong() == 35) { 
				assertTrue("Error: " + pc.getInt(), pc.getInt() > 0 && pc.getInt() < nObj+2);
				n++;
			}
		}
		assertTrue("Objects found: " + n + " expected " + nObj, n == nObj);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
        Extent<TestClass> extent = pm.getExtent(TestClass.class, false);
        Iterator<TestClass> it = extent.iterator();
        int nDel = 0;
        while (it.hasNext()){
            pm.deletePersistent(it.next());
            nDel++;
        }
        assertTrue("Found: " + nDel + " expected" + nObj, nDel >= nObj);
        extent.closeAll();
		
		pm.currentTransaction().commit();

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

		final int nObj = 500000;
		for (int i = 0; i < nObj; i++) {
			TestClassTiny pc = new TestClassTiny();
			pc.setInt(i+1);
			pm.makePersistent(pc);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM();

		//now read it all
		pm = TestTools.openPM();

		pm.currentTransaction().begin();
		
		Extent<TestClassTiny> ex = pm.getExtent(TestClassTiny.class);
		int n = 0;
		for (TestClassTiny pc: ex) {
			assertTrue("pc.getInt()=" + pc.getInt() + "<" + (nObj + 2), 
			        pc.getInt() > 0 && pc.getInt() < nObj+2);
			n++;
		}
		assertTrue("Objects found: " + n, n>=nObj);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		start("deleting objects");
        Extent<TestClassTiny> extent = pm.getExtent(TestClassTiny.class, false);
        Iterator<TestClassTiny> it = extent.iterator();
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

        extent = pm.getExtent(TestClassTiny.class, false);
        Iterator<TestClassTiny> it2 = extent.iterator();
        assertFalse(it2.hasNext());

		
		pm.currentTransaction().commit();

		TestTools.closePM();
	}
	
	/**
	 * Test object ID re-usage does not occur.
	 */
	@Test
	public void testThatOidAreNeverReusedCommit() {
		//First, create object
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClass tc = new TestClass();
		pm.makePersistent(tc);
		Object firstOid = JDOHelper.getObjectId(tc);
		pm.currentTransaction().commit();
		
		//now delete it
		pm.currentTransaction().begin();
		tc = (TestClass) pm.getObjectById(firstOid);
		Object deletedOid = null;
		pm.deletePersistent(tc);
		deletedOid = JDOHelper.getObjectId(tc);
		pm.currentTransaction().commit();

		//create another object
		pm.currentTransaction().begin();
		TestClass tc2 = new TestClass();
		pm.makePersistent(tc2);
		Object oidP = pm.getObjectId(tc2);
		//check that oid differs
		assertFalse("" + deletedOid + " = " + oidP, deletedOid.equals(oidP));
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	/**
	 * Test object ID re-usage does not occur.
	 */
	@Test
	public void testThatOidAreNeverReusedCloseSession() {
		//First, create object
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClass tc = new TestClass();
		pm.makePersistent(tc);
		Object firstOid = JDOHelper.getObjectId(tc);
		pm.currentTransaction().commit();
		
		//now delete it
		pm.currentTransaction().begin();
		tc = (TestClass) pm.getObjectById(firstOid);
		Object deletedOid = null;
		pm.deletePersistent(tc);
		deletedOid = JDOHelper.getObjectId(tc);
		pm.currentTransaction().commit();
		TestTools.closePM();

		//create another object
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClass tc2 = new TestClass();
		pm.makePersistent(tc2);
		Object oidP = pm.getObjectId(tc2);
		//check that oid differs
		assertFalse("" + deletedOid + " = " + oidP, deletedOid.equals(oidP));
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
		TestTools.removeDb();
		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}
}
