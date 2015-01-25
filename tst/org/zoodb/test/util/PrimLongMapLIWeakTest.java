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
package org.zoodb.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;
import org.zoodb.internal.util.PrimLongMap;
import org.zoodb.internal.util.PrimLongMapLIWeak;

/**
 * Test harness for PrimLongTreeMap.
 *
 * @author  Tilmann Zaeschke
 */
public final class PrimLongMapLIWeakTest extends PrimLongMapTest {

	@Override
	protected PrimLongMap<String> createMap() {
		return new PrimLongMapLIWeak<>();
	}
 
    @Test
    public void testLargeWithGC() {
    	int N = 10000;
    	ArrayList<String> pinned = new ArrayList<>();
    	for (int i = 0; i < N; i++) {
    		pinned.add("pinned: " + i);
    	}
    	PrimLongMapLIWeak<String> map = new PrimLongMapLIWeak<>();
    	for (int i = 0; i < N; i++) {
    		map.put(3*N+i, pinned.get(i));
    		map.put(5*N+i, "not-pinned: " + i);
    	}
    	
    	assertEquals(map.size(), 2*N);

    	System.gc();
    	System.gc();
    	System.gc();
    	
    	int n = 0;
    	for (String s: map.values()) {
    		//should never return null
    		assertTrue(s != null);
    		n++;
    	}
    	assertEquals(n, N);
    	assertEquals(map.size(), N);
    	
    	//Check content
    	for (String s: pinned) {
    		assertTrue(map.containsValue(s));
    	}
    	
    	System.out.println("Size: " + n);
    }
    
}
