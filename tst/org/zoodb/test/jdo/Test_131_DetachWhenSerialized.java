/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import javax.jdo.JDOHelper;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class Test_131_DetachWhenSerialized {
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClassSer.class);
	}

	@Before
	public void before() {
		TestTools.dropInstances(TestClassSer.class);
	}
	
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}

	@SuppressWarnings("unchecked")
	private <T> T clone(T object) {
		try (PipedOutputStream pos = new PipedOutputStream();
				PipedInputStream pis = new PipedInputStream(pos);
				ObjectOutput out = new ObjectOutputStream(pos);
				ObjectInput in = new ObjectInputStream(pis)) {
			
			Thread t = new Thread(() -> {
				try {
					out.writeObject(object);
					out.flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
			t.start();

			t.join(1000);

			return (T) in.readObject();
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Test
	public void testDetachPersistent() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClassSer tc1 = new TestClassSer();
		tc1.setMyInt(5);

		checkSerialization(tc1, ObjectState.TRANSIENT);
		
		pm.makePersistent(tc1);

		checkSerialization(tc1, ObjectState.DETACHED_CLEAN);
		
		pm.currentTransaction().commit();

		checkSerialization(tc1, ObjectState.DETACHED_CLEAN);
		
		TestTools.closePM();

		checkSerialization(tc1, ObjectState.DETACHED_CLEAN);
}

	private void checkSerialization(TestClassSer tc1, ObjectState state) {
		TestClassSer x0 = clone(tc1);
		System.out.println(x0.toString());
		JDOHelper.getObjectId(x0);
		assertEquals(state, JDOHelper.getObjectState(x0));
		assertEquals(JDOHelper.getVersion(tc1), JDOHelper.getVersion(x0));
		assertEquals(tc1.jdoZooGetTimestamp(), x0.jdoZooGetTimestamp());
		assertEquals(tc1.jdoZooGetOid(), x0.jdoZooGetOid());
		//TODO
//		assertEquals(JDOHelper.getObjectId(tc1), JDOHelper.getObjectId(x0));
		assertEquals(5, x0.getMyInt());
	}

	@Test
	public void testReatachPersistent() {
		PersistenceManager pm = TestTools.openPM();
		pm.setDetachAllOnCommit(true);
		pm.currentTransaction().begin();
		
		TestClassSer tc1 = new TestClassSer();
		tc1.setMyInt(5);
		
		pm.makePersistent(tc1);

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		TestClassSer x0 = clone(tc1);
		pm.makePersistent(x0);

		pm.currentTransaction().commit();
		System.out.println(tc1.toString());
		System.out.println(x0.toString());

		TestTools.closePM();
	}


}
