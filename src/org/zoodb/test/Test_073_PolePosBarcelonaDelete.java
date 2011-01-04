package org.zoodb.test;

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.data.JB0;
import org.zoodb.test.data.JB1;
import org.zoodb.test.data.JB2;
import org.zoodb.test.data.JB3;
import org.zoodb.test.data.JB4;

public class Test_073_PolePosBarcelonaDelete {

	private static final String DB_NAME = "TestDb";
	private static final int COUNT = 2000;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
		TestTools.defineSchema(DB_NAME, JB0.class);
		TestTools.defineSchema(DB_NAME, JB1.class);
		TestTools.defineSchema(DB_NAME, JB2.class);
		TestTools.defineSchema(DB_NAME, JB3.class);
		TestTools.defineSchema(DB_NAME, JB4.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		for (int i = 1; i <= COUNT; i++) {
			JB4 b4 = new JB4();
			b4.setAll(i);
			pm.makePersistent(b4);
//			JB0 jb;
//			jb = new JB4(4, 4, i, 4, 4);
//			pm.makePersistent(jb);
//			jb = new JB3(3, 3, i, 3);
//			pm.makePersistent(jb);
//			jb = new JB2(2, 2, i);
//			pm.makePersistent(jb);
//			jb = new JB1(1, 2);
//			pm.makePersistent(jb);
//			jb = new JB0(0);
//			pm.makePersistent(jb);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM(pm);
	}


	@Test
	public void testBarcelonaDelete(){
		System.out.println("Testing delete()");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Extent<JB4> extent = pm.getExtent(JB4.class, false);
		Iterator<JB4> it = extent.iterator();
		while(it.hasNext()){
			pm.deletePersistent(it.next());
			//addToCheckSum(5);
		}
		extent.closeAll();
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
	
}
