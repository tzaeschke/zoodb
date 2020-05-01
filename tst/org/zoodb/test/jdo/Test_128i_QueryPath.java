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
