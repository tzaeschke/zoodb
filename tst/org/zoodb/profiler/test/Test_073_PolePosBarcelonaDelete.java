/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.profiler.test;

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.profiler.test.data.JB0;
import org.zoodb.profiler.test.data.JB1;
import org.zoodb.profiler.test.data.JB2;
import org.zoodb.profiler.test.data.JB3;
import org.zoodb.profiler.test.data.JB4;
import org.zoodb.profiler.test.util.TestTools;

public class Test_073_PolePosBarcelonaDelete {

	private static final int COUNT = 2000;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(JB0.class, JB1.class, JB2.class, JB3.class, JB4.class);

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
		TestTools.removeDb();
	}
	
}
