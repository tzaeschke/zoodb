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

import javax.jdo.PersistenceManager;

import org.junit.Before;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.profiler.test.util.TestTools;
import org.zoodb.test.TestClass;
import org.zoodb.test.Test_070_Query;


/**
 * Perform query tests using one indexed attribute.
 * 
 * @author Tilmann Zäschke
 */
public class Test_070i_Query extends Test_070_Query {

	@Before
	public void createIndex() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooSchema.locateClass(pm, TestClass.class);
		if (!s.isIndexDefined("_int")) {
			s.defineIndex("_int", false);
		}
		pm.currentTransaction().commit();
		TestTools.closePM();
	}
	
}
