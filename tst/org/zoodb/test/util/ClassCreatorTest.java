/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.test.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.zoodb.jdo.internal.util.ClassCreator;

public class ClassCreatorTest {

	@Test
	public void test() {
		Class<?> cls1 = ClassCreator.createClass("MyClass");
		assertEquals("MyClass", cls1.getName());
		
		Class<?> cls2 = ClassCreator.createClass("MyClass2", "MyClass");
		assertEquals("MyClass2", cls2.getName());
		assertEquals("MyClass", cls2.getSuperclass().getName());
	}
	
}
