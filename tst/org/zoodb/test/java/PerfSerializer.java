/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
