package org.zoodb.test.java;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.api.ZooConfig;
import org.zoodb.test.TestClass;
import org.zoodb.test.util.TestTools;

public class PerfSerializer {
	
	private static final int MAX_OBJ = 1000000;
	
	public static void main(String[] args) {
		ZooConfig.setFileManager(ZooConfig.FILE_MGR_IN_MEMORY);
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
		for (int i = 0; i < 3; i++) {
			new PerfSerializer().run();
		}
		TestTools.removeDb();
	}

	private void run() {
		
		start("write");
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		for (int i = 0; i < MAX_OBJ; i++) {
			TestClass tc = new TestClass();
			pm.makePersistent(tc);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
		stop("write");
		
		long sum = 0;
		for (int j = 0; j < 2; j++) {
			start("read");
			pm = TestTools.openPM();
			pm.currentTransaction().begin();
			
			
			for (TestClass tc: pm.getExtent(TestClass.class)) {
				sum += tc.getLong();
			}
			pm.currentTransaction().rollback();
			TestTools.closePM();
			stop("read");
		}
		
		System.out.println("sum=" + sum);
	}
	
	
	private double _total = 0;
	private long _time;
	private void start(String msg) {
		_time = System.currentTimeMillis();
	}
	private void stop(String msg) {
		long t = System.currentTimeMillis() - _time;
		double td = t/1000.0;
		_total += td;
		System.out.println(msg + ": " + td + " / " + _total);
	}
}
