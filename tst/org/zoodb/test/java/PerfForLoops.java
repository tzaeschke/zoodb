/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.test.java;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.zoodb.jdo.internal.server.index.PagedLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.internal.util.BucketStack;
import org.zoodb.jdo.internal.util.BucketTreeStack;
import org.zoodb.jdo.internal.util.PrimLongMapLI;


/**
 * Testing collection performance.
 * 
 * Result of iterating over arrays.
 * For arrays[] and and ArrayLists, the fasted way of iterating over them is by using the
 * "for (X x: c)" loops followed by "for (int i=0; i<x.len; i++)".
 * The times vary across Java 6_26, 7_02 and 7_04, e.g. the above loops may use the same time,
 * rather than the first one being faster.
 * 
 * @author Tilmann Zäschke
 */
public class PerfForLoops {

	//private static final int MAX_I = 2000000;
	private static final int MAX_I = 100;
	private static final int N = 100000;
	private static final int NM = 100; //maps

	public static void main(String[] args) {
		new PerfForLoops().run();
	}

	public void run() {
        ArrayList<int[]> matA = new ArrayList<int[]>();
        ArrayList<ArrayList<Integer>> matAL = new ArrayList<ArrayList<Integer>>();
        ArrayList<List<Integer>> matUL = new ArrayList<List<Integer>>();
        ArrayList<Collection<Integer>> matC = new ArrayList<Collection<Integer>>();
	    
	    for (int x = 0; x < 10; x++ ) {
    		ArrayList<Integer> al = new ArrayList<Integer>(MAX_I+x);
    		int[] array = new int[MAX_I+x];
    		for (int i = 0; i < MAX_I+x; i++) {
    		    array[i] = i;
    		    al.add((Integer)i);
    		}
    		matA.add(array);
    		matAL.add(al);
            matUL.add( Collections.unmodifiableList(al) );
            matC.add( al );
	    }

				_useTimer = true;
//				compareInsert(aList, lList, array, Array, bal, bs);
//				compareRemove(aList, lList, array, Array, bal, bs);
//				compareInsert(aList, lList, array, Array, bal, bs);
		
		
				//call sub-method, so hopefully the compiler does not recognize that these are all ArrayLists
				_useTimer = false;
				for (int i = 0; i < 3; i++) {
                    compare(matC, matAL, matUL, matA);
				}
				_useTimer = true;
                compare(matC, matAL, matUL, matA);
                compare(matC, matAL, matUL, matA);
                compare(matC, matAL, matUL, matA);

//		_useTimer = false;
//		for (int i = 0; i < 3; i++) {
//			compare(map, mapId, hMap, lMap, ull, ll);
//		}
//		_useTimer = true;
//		compare(map, mapId, hMap, lMap, ull, ll);

		System.out.println("Done!");
	}


    private void compare(ArrayList<Collection<Integer>> matC, ArrayList<ArrayList<Integer>> matAL,
            ArrayList<List<Integer>> matUL, ArrayList<int[]> matA) {
        int n = 0;
        startTime("array-it");
        for (int[] array: matA)
        for (int x = 0; x < N; x++) {
            for (long b: array) {
                n += b;
            }
        }
        stopTime("array-it");

        startTime("array-for+");
        for (int[] array: matA)
        for (int x = 0; x < N; x++) {
            for (int i = 0; i < array.length; i++) {
                n += array[i];
            }
        }
        stopTime("array-for+");

        startTime("array-for+f");
        for (int[] array: matA) {
        final int size = array.length;
        for (int x = 0; x < N; x++) {
            for (int i = 0; i < size; i++) {
                n += array[i];
            }
        }
        }
        stopTime("array-for+f");

        startTime("array-for-");
        for (int[] array: matA)
        for (int x = 0; x < N; x++) {
            for (int i = array.length-1; i >=0; --i) {
                n += array[i];
            }
        }
        stopTime("array-for-");

        startTime("uList-f");
        for (List<Integer> uList: matUL)
        for (int x = 0; x < N; x++) {
            for (Integer b: uList) {
                n += b;
            }
        }
        stopTime("uList-f");

        startTime("aList-f");
        for (ArrayList<Integer> aList: matAL)
        for (int x = 0; x < N; x++) {
            for (Integer b: aList) {
                n += b;
            }
        }
        stopTime("aList-f");

        startTime("aList-get(i)");
        for (ArrayList<Integer> aList: matAL)
        for (int x = 0; x < N; x++) {
            for (int i = 0; i < aList.size(); i++) {
                n += aList.get(i);
            }
        }
        stopTime("aList-get(i)");

        startTime("aList-it");
        for (ArrayList<Integer> aList: matAL)
        for (int x = 0; x < N; x++) {
            Iterator<Integer> aIt = aList.iterator(); 
            while (aIt.hasNext()) {
                n += aIt.next();
            }
        }
        stopTime("aList-it");
        System.out.println("***");
		
		//ensure that n is not optimized away
		if (n == 0) {
			throw new IllegalStateException();
		}
    }

	private void compareInsert(ArrayList<Long> aList,
			List<Long> lList, long[] array, Long[] Array, BucketTreeStack<Long> bal, 
			BucketStack<Long> bs) {

		startTime("array-i");
		for (int x = 0; x < N; x++) 
			for (int i = 0; i < MAX_I; i++) {
				array[i] = 1;
			}
		stopTime("array-i");

		startTime("Array-i");
		for (int x = 0; x < N; x++) 
			for (int i = 0; i < MAX_I; i++) {
				Array[i] = 1L;
			}
		stopTime("Array-i");

		startTime("aList-i");
		for (int x = 0; x < N; x++) 
			for (int i = 0; i < MAX_I; i++) {
				aList.add(1L);
			}
		stopTime("aList-i");

		startTime("lList-i");
		for (int x = 0; x < N; x++) 
			for (int i = 0; i < MAX_I; i++) {
				lList.add(1L);
			}
		stopTime("lList-i");

		startTime("BAL-i");
		for (int x = 0; x < N; x++) 
			for (int i = 0; i < MAX_I; i++) {
				bal.add(1L);
			}
		stopTime("BAL-i");

		startTime("BS-i");
		for (int x = 0; x < N; x++) 
			for (int i = 0; i < MAX_I; i++) {
				bs.push(1L);
			}
		stopTime("BS-i");
	}


	private void compareRemove(ArrayList<Long> aList,
			List<Long> lList, long[] array, Long[] Array, BucketTreeStack<Long> bal, 
			BucketStack<Long> bs) {

		startTime("array-r");
		for (int x = 0; x < N; x++) 
			for (int i = MAX_I - 1; i >= 0; i--) {
				array[i] = 0;
			}
		stopTime("array-r");

		startTime("Array-r");
		for (int x = 0; x < N; x++) 
			for (int i = MAX_I - 1; i >= 0; i--) {
				Array[i] = 0L;
			}
		stopTime("Array-r");

		startTime("aList-r");
		for (int x = 0; x < N; x++) 
			for (int i = MAX_I - 1; i >= 0; i--) {
				aList.remove(aList.size()-1);
			}
		stopTime("aList-r");

		startTime("lList-r");
		for (int x = 0; x < N; x++) 
			for (int i = MAX_I - 1; i >= 0; i--) {
				lList.remove(lList.size()-1);
			}
		stopTime("lList-r");

		startTime("BAL-r");
		for (int x = 0; x < N; x++) 
			for (int i = MAX_I - 1; i >= 0; i--) {
				bal.removeLast();
			}
		stopTime("BAL-r");

		startTime("BS-r");
		for (int x = 0; x < N; x++) 
			for (int i = MAX_I - 1; i >= 0; i--) {
				bs.pop();
			}
		stopTime("BS-r");
	}


    private void compare(Collection<Integer> coll, ArrayList<Integer> aList,
            List<Integer> uList, long[] array) {
        int n = 0;
        startTime("array-it");
        for (int x = 0; x < N; x++) {
            for (long b: array) {
                n += b;
            }
        }
        stopTime("array-it");

        startTime("array-for+");
        for (int x = 0; x < N; x++) {
            for (int i = 0; i < array.length; i++) {
                n += array[i];
            }
        }
        stopTime("array-for+");

        startTime("array-for+f");
        final int size = array.length;
        for (int x = 0; x < N; x++) {
            for (int i = 0; i < size; i++) {
                n += array[i];
            }
        }
        stopTime("array-for+f");

        startTime("array-for-");
        for (int x = 0; x < N; x++) {
            for (int i = array.length-1; i >=0; --i) {
                n += array[i];
            }
        }
        stopTime("array-for-");

        startTime("uList-f");
        for (int x = 0; x < N; x++) {
            for (Integer b: uList) {
                n += b;
            }
        }
        stopTime("uList-f");

        startTime("aList-f");
        for (int x = 0; x < N; x++) {
            for (Integer b: aList) {
                n += b;
            }
        }
        stopTime("aList-f");

        startTime("aList-get(i)");
        for (int x = 0; x < N; x++) {
            for (int i = 0; i < aList.size(); i++) {
                n += aList.get(i);
            }
        }
        stopTime("aList-get(i)");

        startTime("aList-it");
        for (int x = 0; x < N; x++) {
            Iterator<Integer> aIt = aList.iterator(); 
            while (aIt.hasNext()) {
                n += aIt.next();
            }
        }
        stopTime("aList-it");
		
		//ensure that n is not optimized away
		if (n == 0) {
			throw new IllegalStateException();
		}
    }

	private void compare(Map<Long, Long> map, Map<Long, Long> mapId, HashMap<Long, Long> hMap, 
			PrimLongMapLI<Long> lMap, PagedUniqueLongLong ull, PagedLongLong ll) {
		int n = 0;
		startTime("map-keyset-f");
		for (int x = 0; x < NM; x++) {
			for (Long b: map.keySet()) {
				n += b;
			}
		}
		stopTime("map-keyset-f");

		startTime("mapID-keyset-f");
		for (int x = 0; x < NM; x++) {
			for (Long b: mapId.keySet()) {
				n += b;
			}
		}
		stopTime("mapID-keyset-f");

		startTime("hMap-keyset-f");
		for (int x = 0; x < NM; x++) {
			for (Long b: hMap.keySet()) {
				n += b;
			}
		}
		stopTime("hMap-keyset-f");

		startTime("lMap-keyset-f");
		for (int x = 0; x < NM; x++) {
			for (Long b: lMap.keySet()) {
				n += b;
			}
		}
		stopTime("lMap-keyset-f");

		startTime("lMap-keyset-it");
		for (int x = 0; x < NM; x++) {
			Iterator<Long> aIt = lMap.keySet().iterator(); 
			while (aIt.hasNext()) {
				n += aIt.next();
			}
		}
		stopTime("lMap-keyset-it");

		startTime("lMap-entry-f");
		for (int x = 0; x < NM; x++) {
			for (PrimLongMapLI.Entry<Long> e: lMap.entrySet()) {
				n += e.getKey();
			}
		}
		stopTime("lMap-entry-f");

		startTime("lMap-entry-it");
		for (int x = 0; x < NM; x++) {
			Iterator<PrimLongMapLI.Entry<Long>> aIt = lMap.entrySet().iterator(); 
			while (aIt.hasNext()) {
				n += aIt.next().getKey();
			}
		}
		stopTime("lMap-entry-it");

		startTime("ull-it");
		for (int x = 0; x < NM; x++) {
			Iterator<LLEntry> aIt = ull.iterator(Long.MIN_VALUE, Long.MAX_VALUE); 
			while (aIt.hasNext()) {
				n += aIt.next().getKey();
			}
		}
		stopTime("ull-it");

		startTime("ll-it");
		for (int x = 0; x < NM; x++) {
			Iterator<LLEntry> aIt = ll.iterator(Long.MIN_VALUE, Long.MAX_VALUE); 
			while (aIt.hasNext()) {
				n += aIt.next().getKey();
			}
		}
		stopTime("ll-it");
		
		//ensure that n is not optimized away
		if (n == 0) {
			throw new IllegalStateException();
		}
	}

	// timing

	private long _time;
	private boolean _useTimer;

	private void startTime(String msg) {
		_time = System.currentTimeMillis();
	}

	private void stopTime(String msg) {
		long diff = System.currentTimeMillis() - _time;
		double time = diff/1000.0;
		if (_useTimer) {
			System.out.println(msg + ": " + time + "s");
		}
	}
}
