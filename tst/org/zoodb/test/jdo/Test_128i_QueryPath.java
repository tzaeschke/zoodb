/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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

import org.junit.BeforeClass;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query paths.
 * 
 * @author ztilmann
 *
 */
public class Test_128i_QueryPath extends Test_128_QueryPath {

	@BeforeClass
	public static void setUp() {
		Test_128_QueryPath.setUp();
		TestTools.defineIndex(TestClass.class, "_ref2", false);
		TestTools.defineIndex(TestClass.class, "_string", false);
	}
}
