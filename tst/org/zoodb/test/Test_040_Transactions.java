/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import static org.junit.Assert.fail;

import java.util.Properties;

import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

public class Test_040_Transactions {

	private static final String DB_NAME = "TestDb";
	
	private PersistenceManager pm;
	private PersistenceManagerFactory pmf;
	
	@BeforeClass
	public static void setUp() {
		TestTools.createDb(DB_NAME);
	}

	@Test
	public void testTransaction() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();

		//test before begin()
		try {
			pm.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		try {
			pm.currentTransaction().rollback();
			fail();
		} catch (JDOUserException e) {
			//good
		}

		//begin -> commit
		pm.currentTransaction().begin();
		try {
			pm.currentTransaction().begin();
			fail();
		} catch (JDOUserException e) {
			//good
		}

		pm.currentTransaction().commit();
		try {
			pm.currentTransaction().commit();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//begin -> rollback
		pm.currentTransaction().begin();
		pm.currentTransaction().rollback();
		try {
			pm.currentTransaction().rollback();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		
		pm.currentTransaction().begin();
		try {
			pm.close();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		try {
			pmf.close();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		//close
		pm.currentTransaction().rollback();
		pm.close();
		try {
			pm.currentTransaction();//.begin();
			fail();
		} catch (JDOFatalUserException e) {
			//good
		}
		
		pmf.close();
	}
	
	@Test
	public void testClosedTransaction() {
		Properties props = new ZooJdoProperties(DB_NAME);
		pmf = JDOHelper.getPersistenceManagerFactory(props);
		pm = pmf.getPersistenceManager();

		pm.close();
		pmf.close();

		
		try {
			pm.currentTransaction();
			fail();
		} catch (JDOFatalUserException e) {
			//good
		}

		try {
			pm.makePersistent(new TestClass());
			fail();
		} catch (JDOFatalUserException e) {
			//good
		}
		
		//TODO
		System.out.println("TODO check others on closed PM");
	}

	
	@After
	public void afterTest() {
		if (pm != null && !pm.isClosed()) {
			if (pm.currentTransaction().isActive()) {
				pm.currentTransaction().rollback();
			}
			pm.close();
		}
		pm = null;
		if (pmf != null && !pmf.isClosed()) {
			pmf.close();
		}
		pmf = null;
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb(DB_NAME);
	}
}
