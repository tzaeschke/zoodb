/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.test.java;

import javax.jdo.PersistenceManager;

import org.zoodb.test.jdo.TestClass;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooConfig;

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
