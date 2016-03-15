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
package org.zoodb.test.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;
import org.zoodb.internal.util.PrimLongMap;
import org.zoodb.internal.util.PrimLongMapLISoft;

/**
 * Test harness for PrimLongTreeMap.
 *
 * @author  Tilmann Zaeschke
 */
public final class PrimLongMapLISoftTest extends PrimLongMapTest {

	@Override
	protected PrimLongMap<String> createMap() {
		return new PrimLongMapLISoft<>();
	}


 
    @Test
    public void testLargeWithGC() {
    	int N = 10000;
    	ArrayList<String> pinned = new ArrayList<>();
    	for (int i = 0; i < N; i++) {
    		pinned.add("pinned: " + i);
    	}
    	PrimLongMapLISoft<String> map = new PrimLongMapLISoft<>();
    	for (int i = 0; i < N; i++) {
    		map.put(3*N+i, pinned.get(i));
    		map.put(5*N+i, "not-pinned: " + i);
    	}
    	
    	System.gc();
    	System.gc();
    	System.gc();
    	
    	int n = 0;
    	for (String s: map.values()) {
    		//should never return null
    		assertTrue(s != null);
    		n++;
    	}
    	assertTrue(n >= N);
    	//assertTrue(n <= 2*N);
    	assertTrue(n <= 2*N);
    	
    	//Check content
    	for (String s: pinned) {
    		assertTrue(map.containsValue(s));
    	}
    	
    }
    
}
