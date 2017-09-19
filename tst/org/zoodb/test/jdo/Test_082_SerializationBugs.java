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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import javax.jdo.JDOFatalException;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.schema.ZooHandle;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.testutil.TestTools;

public class Test_082_SerializationBugs {


	@Before
	public void before() {
		// nothing
	}


	/**
	 * Run after each test.
	 */
	@After
	public void after() {
		TestTools.closePM();
	}


	@BeforeClass
	public static void beforeClass() {
		TestTools.createDb();
		TestTools.defineSchema(TwitSession.class);
	}


	@AfterClass
	public static void afterClass() {
		TestTools.removeDb();
	}


	/**
	 * Test serialisation. 
	 * 
	 * This used to fail because null-Date fields were wrongly serialised.
	 */
	@Test
	public void testSerialization() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TwitSession s = new TwitSession("mySession");
		pm.makePersistent(s);
		Object oid = pm.getObjectId(s);
		pm.currentTransaction().commit();
		TestTools.closePM();

		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TwitSession s2 = (TwitSession) pm.getObjectById(oid);
		assertNotNull(s2);
		pm.currentTransaction().commit();
		pm.close();
	}

	/**
	 * Test missing no-arg constructor. 
	 * 
	 * This used to return a JDOObjectNotFoundException.
	 */
	@Test
	public void testNoArgError() {
		TestTools.defineSchema(NoConstructorTest.class);
		TestTools.defineIndex(NoConstructorTest.class, "name", true);
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		NoConstructorTest s = new NoConstructorTest("mySession");
		pm.makePersistent(s);
		Object oid = pm.getObjectId(s);
		pm.currentTransaction().commit();

		//try w/o new tx
		pm.currentTransaction().begin();
		NoConstructorTest s2 = null;
		try {
			s2 = (NoConstructorTest) pm.getObjectById(oid);
			assertNotNull(s2);
			assertEquals("mySession", s2.getName());
			assertNotNull(s2.getSCO());
			fail();
		} catch (JDOFatalException e) {
			//good
		}
		pm.currentTransaction().commit();
		pm.close();
		TestTools.closePM();

		//try with new tx (fails differently!)
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		try {
			s2 = (NoConstructorTest) pm.getObjectById(oid);
			assertNotNull(s2);
			assertEquals("mySession", s2.getName());
			assertNotNull(s2.getSCO());
			fail();
		} catch (JDOFatalException e) {
			//good
		}
		pm.currentTransaction().commit();
		pm.close();

		//try with new tx and GO
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooSchema zs = ZooJdoHelper.schema(pm); 
		try {
			ZooHandle zh = zs.getHandle(oid);
			assertNotNull(zh);
			fail();
		} catch (JDOFatalException e) {
			//good
		}
		pm.currentTransaction().commit();
		pm.close();
	}

	/**
	 * Test serialisation of innocent looking anonymous SCO. 
	 * 
	 * --> This should not work!
	 */
	@Test
	public void testSerializationOfInnocentAnonymousSCO() {
		TestTools.defineSchema(InnocentSCO.class);
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		InnocentSCO s = new InnocentSCO(new HashSet<String>() {
			private static final long serialVersionUID = 1L;
			{
				add("Hello");
			}
		});
		pm.makePersistent(s);
		try {
			pm.currentTransaction().commit();
		} catch (JDOUserException e) {
			assertTrue(e.getMessage().contains("nonymous"));
		}
		TestTools.closePM();
	}


}


@SuppressWarnings("unused")
class TwitSession extends PersistenceCapableImpl {
	private String name;
	//private User creator;
	private Date creationDate;
	//private Device creationDevice;
	private boolean isDeleted;
	//private User deletor;
	private Date deletionDate;
	//private Device deletionDevice;
	private Map<Long, Deque<Object>> twits;
	private Deque<Object> log;

	protected TwitSession() {
		// no-argument constructor for zoodb.
	}

	public TwitSession(final String name) {//, final User creator, final Device device) {
		this.name = name;
		//this.creator = creator;
		//this.creationDevice = device;
		this.creationDate = Calendar.getInstance().getTime();
		this.twits = new HashMap<Long, Deque<Object>>();
		this.log = new LinkedList<Object>();
		this.isDeleted = false;
	}
}

class NoConstructorTest extends PersistenceCapableImpl {
	private String name;
	private NoConstructorSCO sco;

	@SuppressWarnings("unused")
	private NoConstructorTest() {
		// JDO
	}
	
	public NoConstructorTest(final String name) {
		this.sco = new NoConstructorSCO(name);
		this.name = name;
	}
	public String getName() {
		zooActivateRead();
		return name;
	}
	public NoConstructorSCO getSCO() {
		zooActivateRead();
		return sco;
	}
}

class NoConstructorSCO {
	private String name;

	// no no-arg constructor
	
	public NoConstructorSCO(final String name) {
		this.name = name;
	}
	public String getName() {
		return name;
	}
}

class InnocentSCO extends PersistenceCapableImpl {
	private HashSet<String> names;

	@SuppressWarnings("unused")
	private InnocentSCO() {
		// JDO
	}
	
	public InnocentSCO(HashSet<String> names) {
		this.names = names;
	}
	public HashSet<String> getNames() {
		zooActivateRead();
		return names;
	}
}

