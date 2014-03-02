/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import javax.jdo.PersistenceManager;

import org.junit.Before;
import org.zoodb.jdo.ZooJdoSchema;
import org.zoodb.schema.ZooClass;
import org.zoodb.test.testutil.TestTools;


/**
 * Perform query tests using one indexed attribute.
 * 
 * @author Tilmann Zaeschke
 */
public class Test_070i_Query extends Test_070_Query {

	@Before
	public void createIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooJdoSchema.locateClass(pm, TestClass.class);
		if (!s.hasIndex("_int")) {
			s.createIndex("_int", false);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
}
