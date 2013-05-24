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
package org.zoodb.test;

import static org.junit.Assert.assertNotNull;

import java.util.Calendar;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.test.testutil.TestTools;

public class Test_082_SerailizationBugs {


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

