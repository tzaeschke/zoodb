package org.zoodb.test;

import static org.junit.Assert.*;

import java.util.ArrayList;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.internal.ZooHandle;
import org.zoodb.test.data.JB0;
import org.zoodb.test.data.JB1;
import org.zoodb.test.data.JB2;
import org.zoodb.test.data.JB3;
import org.zoodb.test.data.JB4;
import org.zoodb.test.data.JdoIndexedPilot;
import org.zoodb.test.data.JdoPilot;
import org.zoodb.test.util.TestTools;

public class Test_031_SchemaReading {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass.define(pm, TestClass.class);
		ZooClass.define(pm, JdoPilot.class);
		ZooClass.define(pm, JB0.class);
		ZooClass.define(pm, JB1.class);
		ZooClass.define(pm, JB2.class);
		ZooClass.define(pm, JB3.class);
		ZooClass.define(pm, JB4.class);
		ZooClass.define(pm, JdoIndexedPilot.class);
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
	    TestTools.closePM();
		TestTools.removeDb();
	}

	@After
	public void after() {
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaZooHandle() {
		PersistenceManager pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		
		TestClass t1 = new TestClass();
		t1.setData(1234567, true, 'x', (byte)126, (short)32000, 12345678901L, "haha", 
				new byte[]{1, 2, 3});
		TestClass t2 = new TestClass();
		t2.setData(-1234567, true, 'y', (byte)-126, (short)-32000, -12345678901L, "hihi", 
				new byte[]{-1, -2, -3});
		
		pm0.makePersistent(t1);
		pm0.makePersistent(t2);
		
		long oid1 = (Long) pm0.getObjectId(t1);
		long oid2 = (Long) pm0.getObjectId(t2);
		
		pm0.currentTransaction().commit();
		TestTools.closePM();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s01 = ZooClass.locate(pm, TestClass.class.getName());
		assertNotNull(s01);

		//closed pm
		try {
			ZooClass.getHandle(pm0, oid1);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good!
		}
		
		//wrong oid
		try {
			ZooClass.getHandle(pm0, 12345678);
			fail();
		} catch (JDOObjectNotFoundException e) {
			//good!
		}
		
		ZooHandle hdl1 = ZooClass.getHandle(pm, oid1);
		ZooHandle hdl2 = ZooClass.getHandle(pm, oid2);
		assertNotNull(hdl1);
		assertNotNull(hdl2);
		
		assertEquals((Long)oid1, (Long)hdl1.getOid());
		assertEquals(pm, hdl1.getSession().getPersistenceManager());
		assertEquals(s01, hdl1.getSchemaHandle());
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaZooHandleReader() {
		PersistenceManager pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		
		TestClass t1 = new TestClass();
		t1.setData(1234567, true, 'x', (byte)126, (short)32000, 12345678901L, "haha", 
				new byte[]{1, 2, 3});
		TestClass t2 = new TestClass();
		t2.setData(-1234567, true, 'y', (byte)-126, (short)-32000, -12345678901L, "hihi", 
				new byte[]{-1, -2, -3});
		
		t1.setRef2(t2);
		
		pm0.makePersistent(t1);
		pm0.makePersistent(t2);
		
		long oid1 = (Long) pm0.getObjectId(t1);
		long oid2 = (Long) pm0.getObjectId(t2);
		
		pm0.currentTransaction().commit();
		TestTools.closePM();
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s01 = ZooClass.locate(pm, TestClass.class.getName());
		assertNotNull(s01);

		ZooHandle hdl1 = ZooClass.getHandle(pm, oid1);
		ZooHandle hdl2 = ZooClass.getHandle(pm, oid2);

		//TODO
//		TestTools.closePM();
//		fail();
		
		assertEquals(126, hdl1.getAttrByte("_byte"));
		assertEquals(1234567, hdl1.getAttrInt("_int"));
		assertEquals(true, hdl1.getAttrBool("_bool"));
		assertEquals('x', hdl1.getAttrChar("_char"));
		assertEquals(32000, hdl1.getAttrShort("_short"));
		assertEquals(12345678901L, hdl1.getAttrLong("_long"));
//TODO		assertEquals("haha", hdl1.getAttrString("_string"));
		
		long oid2b = hdl1.getAttrRefOid("_ref2");
		assertEquals(oid2, oid2b);
		
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaZooHandleReaderOnRenamed() {
		PersistenceManager pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		
		TestClass t1 = new TestClass();
		t1.setData(1234567, true, 'x', (byte)126, (short)32000, 12345678901L, "haha", 
				new byte[]{1, 2, 3});
		TestClass t2 = new TestClass();
		t2.setData(-1234567, true, 'y', (byte)-126, (short)-32000, -12345678901L, "hihi", 
				new byte[]{-1, -2, -3});
		
		t1.setRef2(t2);
		
		pm0.makePersistent(t1);
		pm0.makePersistent(t2);
		
		long oid1 = (Long) pm0.getObjectId(t1);
		long oid2 = (Long) pm0.getObjectId(t2);
		
		pm0.currentTransaction().commit();
		TestTools.closePM();
		
		//rename schema
		pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		ZooClass s = ZooClass.locate(pm0, TestClass.class);
		s.rename("x");
		pm0.currentTransaction().commit();
		TestTools.closePM();

		
		//test renamed
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooClass s01 = ZooClass.locate(pm, "x");
		assertNotNull(s01);

		ZooHandle hdl1 = ZooClass.getHandle(pm, oid1);
		ZooHandle hdl2 = ZooClass.getHandle(pm, oid2);

		//TODO
//		TestTools.closePM();
//		fail();
		
		assertEquals(126, hdl1.getAttrByte("_byte"));
		assertEquals(1234567, hdl1.getAttrInt("_int"));
		assertEquals(true, hdl1.getAttrBool("_bool"));
		assertEquals('x', hdl1.getAttrChar("_char"));
		assertEquals(32000, hdl1.getAttrShort("_short"));
		assertEquals(12345678901L, hdl1.getAttrLong("_long"));
//TODO		assertEquals("haha", hdl1.getAttrString("_string"));
		
		long oid2b = hdl1.getAttrRefOid("_ref2");
		assertEquals(oid2, oid2b);
		
		TestTools.closePM();
		
		//rename back
		pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		s = ZooClass.locate(pm0, "x");
		s.rename(TestClass.class.getName());
		pm0.currentTransaction().commit();
		TestTools.closePM();
	}
	

	//Check that the handle attribute access works across page boundaries.
	@Test
	public void testSchemaZooHandleReaderWithPageWrap() {
		//populate
		PersistenceManager pm0 = TestTools.openPM();
		pm0.currentTransaction().begin();
		ArrayList<Long> oids = new ArrayList<Long>();
		for (int i = 0; i < 10000; i++) {
			TestClass t1 = new TestClass();
			t1.setData(1234567, true, 'x', (byte)126, (short)32000, 12345678901L, "haha", 
					new byte[]{1, 2, 3});
			t1.setRef2(t1);
			pm0.makePersistent(t1);
			oids.add( (Long)pm0.getObjectId(t1) );
		}
		pm0.currentTransaction().commit();
		TestTools.closePM();
		
		//check
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		for (Long oid: oids) {
			ZooHandle hdl1 = ZooClass.getHandle(pm, oid);
			assertEquals(126, hdl1.getAttrByte("_byte"));
			assertEquals(1234567, hdl1.getAttrInt("_int"));
			assertEquals(true, hdl1.getAttrBool("_bool"));
			assertEquals('x', hdl1.getAttrChar("_char"));
			assertEquals(32000, hdl1.getAttrShort("_short"));
			assertEquals(12345678901L, hdl1.getAttrLong("_long"));
			//TODO		assertEquals("haha", hdl1.getAttrString("_string"));
			long oid2 = hdl1.getAttrRefOid("_ref2");
			assertEquals((long)oid, oid2);
		}
		
		TestTools.closePM();
	}
	

	@Test
	public void testSchemaDeletion() {
		System.out.println("Testing Schema deletion - TODO"); //TODO
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		//ensure schema not in DB, only in cache
		ZooClass s01 = ZooClass.locate(pm, TestClass.class.getName());
		assertNotNull(s01);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
		
	
	@Test
	public void testSchemaHierarchy() {
		//test that allocating 6 schemas does not require too many pages //TODO?
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		assertNotNull( ZooClass.locate(pm, JB0.class) );
		assertNotNull( ZooClass.locate(pm, JB1.class) );
		assertNotNull( ZooClass.locate(pm, JB2.class) );
		assertNotNull( ZooClass.locate(pm, JB3.class) );
		assertNotNull( ZooClass.locate(pm, JB4.class) );
		
		JB4 jb4 = new JB4();
		pm.makePersistent(jb4);
		JB0 jb0 = new JB0();
		pm.makePersistent(jb0);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
}
