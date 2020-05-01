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
