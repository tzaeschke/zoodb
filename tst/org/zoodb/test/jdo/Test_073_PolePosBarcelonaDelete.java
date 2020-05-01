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
package org.zoodb.test.jdo;

import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.jdo.classes.TC0;
import org.zoodb.test.jdo.classes.TC1;
import org.zoodb.test.jdo.classes.TC2;
import org.zoodb.test.jdo.classes.TC3;
import org.zoodb.test.jdo.classes.TC4;
import org.zoodb.test.testutil.TestTools;

public class Test_073_PolePosBarcelonaDelete {

	private static final int COUNT = 2000;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TC0.class, TC1.class, TC2.class, TC3.class, TC4.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		for (int i = 1; i <= COUNT; i++) {
			TC4 b4 = new TC4(i, i, i, i, i);
			pm.makePersistent(b4);
//			TC0 jb;
//			jb = new TC4(4, 4, i, 4, 4);
//			pm.makePersistent(jb);
//			jb = new TC3(3, 3, i, 3);
//			pm.makePersistent(jb);
//			jb = new TC2(2, 2, i);
//			pm.makePersistent(jb);
//			jb = new TC1(1, 2);
//			pm.makePersistent(jb);
//			jb = new TC0(0);
//			pm.makePersistent(jb);
		}
		
		pm.currentTransaction().commit();
		TestTools.closePM(pm);
	}


	@Test
	public void testBarcelonaDelete(){
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		Extent<TC4> extent = pm.getExtent(TC4.class, false);
		Iterator<TC4> it = extent.iterator();
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
