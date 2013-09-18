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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.util.Collection;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query setResult().
 * 
 * @author ztilmann
 *
 */
public class Test_120_QuerySetResult {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32001, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)126, (short)32002, 1234567890L, "xyz", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'e', (byte)125, (short)32003, 1234567890L, "xyz", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)124, (short)32004, 1234567890L, "xyz", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)123, (short)32005, 1234567890L, "xyz", new byte[]{1,2},
        		11.1f, -35);
        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        TestTools.closePM();;
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
	public void testFailuresSetResult() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setResult(null);
		q.execute();

		q.setResult("");
		q.execute();
		
		q.setResult(" ");
		q.execute();
		
		checkSetResultFails(pm, "avg()");
		
		checkSetResultFails(pm, "avg(_int), ");
		
		checkSetResultFails(pm, ", avg(_int)");
		
		checkSetResultFails(pm, " , ");
		
		checkSetResultFails(pm, "avg(_int), _long");
		
		checkSetResultFails(pm, "_long, avg(_int)");
		
		checkSetResultFails(pm, "avg()");
		
		checkSetResultFails(pm, "avg()");
		
		checkSetResultFails(pm, "avg()");
	}
	
	private void checkSetResultFails(PersistenceManager pm, String s) {
		Query q = pm.newQuery(TestClass.class);
		q.setResult(s);
		try {
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
	}
	
	/**
     * AVG always returns the type of the aggregated field ?!?!
     */
    @Test
    public void testAVG() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setResult("avg(iDontKnow)");
		try {
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//try internal extent
		q = pm.newQuery(TestClass.class);
		q.setResult("avg(_byte)");
		byte avg = (Byte)q.execute();
		assertEquals(125, avg);
		
		q = pm.newQuery(TestClass.class, "_int > 0");
		q.setResult("avg(_byte)");
		avg = (Byte)q.execute();
		assertEquals(125, avg);
		
		q.setResult("avg(_byte), avg(_int), avg(_short), avg(_long), avg(_char), " +
				"avg(_float), avg(_double)");
		Object[] avgs = (Object[])q.execute();
		assertEquals((byte)125, avgs[0]);
		assertEquals((int)2743, avgs[1]);
		assertEquals((short)32003, avgs[2]);
		assertEquals((long)1234567890L, avgs[3]);
		assertEquals((char)'e', avgs[4]);
		assertEquals(Float.class, avgs[5].getClass());
		assertTrue(2. < (Float)avgs[5] && 2.5 > (Float)avgs[5]);
		assertEquals(Double.class, avgs[6].getClass());
		assertTrue(7. < (Double)avgs[6] && 8. > (Double)avgs[6]);
		
		TestTools.closePM();
    }

    /**
     * SUM returns long/double for integral/float types.
     */
    @Test
    public void testSUM() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setResult("sum(iDontKnow)");
		try {
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//try internal extent
		q = pm.newQuery(TestClass.class);
		q.setResult("sum(_byte)");
		long sum = (Long)q.execute();
		assertEquals(625, sum);
		
		q = pm.newQuery(TestClass.class, "_int > 0");
		q.setResult("sum(_byte)");
		sum = (Long)q.execute();
		assertEquals(625, sum);
		
		q.setResult("sum(_byte), sum(_int), sum(_short), sum(_long), sum(_char), " +
				"sum(_float), sum(_double)");
		Object[] sums = (Object[])q.execute();
		assertEquals((long)625, sums[0]);
		assertEquals((long)13715, sums[1]);
		assertEquals((long)160015, sums[2]);
		assertEquals((long)6172839450L, sums[3]);
		assertEquals((long)505, sums[4]);
		assertEquals(Double.class, sums[5].getClass());
		assertTrue(11. < (Double)sums[5] && 12. > (Double)sums[5]);
		assertEquals(Double.class, sums[6].getClass());
		assertTrue(36. < (Double)sums[6] && 37. > (Double)sums[6]);
		
		TestTools.closePM();
    }
	
	/**
     * MIN always returns the type of the aggregated field.
     */
    @Test
    public void testMIN() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setResult("min(iDontKnow)");
		try {
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//try internal extent
		q = pm.newQuery(TestClass.class);
		q.setResult("min(_byte)");
		byte min = (Byte)q.execute();
		assertEquals(123, min);
		
		q = pm.newQuery(TestClass.class, "_int > 0");
		q.setResult("min(_byte)");
		min = (Byte)q.execute();
		assertEquals(123, min);
		
		q.setResult("min(_byte), min(_int), min(_short), min(_long), min(_char), " +
				"min(_float), min(_double)");
		Object[] mins = (Object[])q.execute();
		assertEquals((byte)123, mins[0]);
		assertEquals((int)1, mins[1]);
		assertEquals((short)32001, mins[2]);
		assertEquals((long)1234567890L, mins[3]);
		assertEquals((char)'c', mins[4]);
		assertEquals(Float.class, mins[5].getClass());
		assertTrue(-2. < (Float)mins[5] && -1 > (Float)mins[5]);
		assertEquals(Double.class, mins[6].getClass());
		assertTrue(-36. < (Double)mins[6] && -34. > (Double)mins[6]);
		
		TestTools.closePM();
    }

	/**
     * MAX always returns the type of the aggregated field.
     */
    @Test
    public void testMAX() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setResult("max(iDontKnow)");
		try {
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//try internal extent
		q = pm.newQuery(TestClass.class);
		q.setResult("max(_byte)");
		byte max = (Byte)q.execute();
		assertEquals(127, max);
		
		q = pm.newQuery(TestClass.class, "_int > 0");
		q.setResult("max(_byte)");
		max = (Byte)q.execute();
		assertEquals(127, max);
		
		q.setResult("max(_byte), max(_int), max(_short), max(_long), max(_char), " +
				"max(_float), max(_double)");
		Object[] maxs = (Object[])q.execute();
		assertEquals((byte)127, maxs[0]);
		assertEquals((int)12345, maxs[1]);
		assertEquals((short)32005, maxs[2]);
		assertEquals((long)1234567890L, maxs[3]);
		assertEquals((char)'g', maxs[4]);
		assertEquals(Float.class, maxs[5].getClass());
		assertTrue(11. < (Float)maxs[5] && 12. > (Float)maxs[5]);
		assertEquals(Double.class, maxs[6].getClass());
		assertTrue(34. < (Double)maxs[6] && 36. > (Double)maxs[6]);
		
		TestTools.closePM();
    }

    @Test
    public void testCOUNT() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setResult("count(iDontKnow)");
		try {
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//try internal extent
		q = pm.newQuery(TestClass.class);
		q.setResult("count(_byte)");
		long cnt = (Long)q.execute();
		assertEquals(5, cnt);
		
		q = pm.newQuery(TestClass.class, "_int > 0");
		q.setResult("count(_byte)");
		cnt = (Long)q.execute();
		assertEquals(5, cnt);
		
		q.setResult("count(_byte), count(_int), count(_short), count(_long), count(_char), " +
				"count(_float), count(_double)");
		Object[] sums = (Object[])q.execute();
		assertEquals((long)5, sums[0]);
		assertEquals((long)5, sums[1]);
		assertEquals((long)5, sums[2]);
		assertEquals((long)5, sums[3]);
		assertEquals((long)5, sums[4]);
		assertEquals((long)5, sums[5]);
		assertEquals((long)5, sums[6]);
		
		TestTools.closePM();
    }
	
    @Test
    public void testFIELD() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setResult("iDontKnow");
		try {
			q.execute();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//try internal extent
		q = pm.newQuery(TestClass.class);
		q.setResult("_byte");
		Collection<?> r = (Collection<?>)q.execute();
		assertTrue(123 <= (Byte)r.iterator().next());
		assertEquals(5, r.size());
		
		q = pm.newQuery(TestClass.class, "_int > 0");
		q.setResult("_byte");
		r = (Collection<?>) q.execute();
		assertTrue(123 <= (Byte)r.iterator().next());
		assertEquals(5, r.size());
		
		q.setResult("_byte, _int, _short, _long, _char, _float, _double");
		r = (Collection<?>) q.execute();
		Object[] avgs = (Object[]) r.iterator().next();
		assertEquals(7, avgs.length);
		assertEquals((byte)127, avgs[0]);
		assertEquals((int)1, avgs[1]);
		assertEquals((short)32001, avgs[2]);
		assertEquals((long)1234567890L, avgs[3]);
		assertEquals((char)'c', avgs[4]);
		assertEquals(Float.class, avgs[5].getClass());
		assertTrue(-2. < (Float)avgs[5] && -1. > (Float)avgs[5]);
		assertEquals(Double.class, avgs[6].getClass());
		assertTrue(34. < (Double)avgs[6] && 36. > (Double)avgs[6]);
		
		TestTools.closePM();
    }
	
}
