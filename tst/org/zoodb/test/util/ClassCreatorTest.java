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
package org.zoodb.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.zoodb.internal.util.ClassCreator;

public class ClassCreatorTest {

	@Test
	public void test() {
		Class<?> cls1 = ClassCreator.createClass("MyClass");
		assertEquals("MyClass", cls1.getName());
		
		Class<?> cls2 = ClassCreator.createClass("MyClass2", "MyClass");
		assertEquals("MyClass2", cls2.getName());
		assertEquals("MyClass", cls2.getSuperclass().getName());
		
		Class<?> cls3 = ClassCreator.createClass("org.xyz.MyClass3", "MyClass2");
		assertEquals("org.xyz.MyClass3", cls3.getName());
		assertEquals("MyClass2", cls3.getSuperclass().getName());
		
		Class<?> cls4 = ClassCreator.createClass("org.xyz.MyClass4", "org.xyz.MyClass3");
		assertEquals("org.xyz.MyClass4", cls4.getName());
		assertEquals("org.xyz.MyClass3", cls4.getSuperclass().getName());
		
		Class<?> cls5 = ClassCreator.createClass("org.xyz.MyClass3", "MyClass2");
		assertTrue(cls5 == cls3);
	}
	
}
