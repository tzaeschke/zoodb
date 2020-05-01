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
package org.zoodb.test.java;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.zoodb.internal.util.BucketStack;
import org.zoodb.internal.util.BucketTreeStack;
import org.zoodb.internal.util.PrimLongArrayList;
import org.zoodb.internal.util.PrimLongArrayList.LongIterator;


/**
 * Testing collection performance.
 * 
 * Result of iterating over arrays.
 * For arrays[] and and ArrayLists, the fasted way of iterating over them is by using the
 * "for (X x: c)" loops followed by "for (int i=0; i<x.len; i++)".
 * The times vary across Java 6_26, 7_02 and 7_04, e.g. the above loops may use the same time,
 * rather than the first one being faster.
 * 
 * @author Tilmann Zaeschke
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
        ArrayList<PrimLongArrayList> matPL = new ArrayList<PrimLongArrayList>();
	    
	    for (int x = 0; x < 10; x++ ) {
    		ArrayList<Integer> al = new ArrayList<Integer>(MAX_I+x);
    		PrimLongArrayList pl = new PrimLongArrayList();
    		int[] array = new int[MAX_I+x];
    		for (int i = 0; i < MAX_I+x; i++) {
    		    array[i] = i;
    		    al.add((Integer)i);
    		    pl.add(i);
    		}
    		matA.add(array);
    		matAL.add(al);
            matUL.add( Collections.unmodifiableList(al) );
            matC.add( al );
            matPL.add( pl );
	    }

				_useTimer = true;
//				compareInsert(aList, lList, array, Array, bal, bs);
//				compareRemove(aList, lList, array, Array, bal, bs);
//				compareInsert(aList, lList, array, Array, bal, bs);
		
		
				//call sub-method, so hopefully the compiler does not recognize that these are all ArrayLists
				_useTimer = false;
				for (int i = 0; i < 3; i++) {
                    compare(matC, matAL, matUL, matA, matPL);
				}
				_useTimer = true;
                compare(matC, matAL, matUL, matA, matPL);
                compare(matC, matAL, matUL, matA, matPL);
                compare(matC, matAL, matUL, matA, matPL);

		System.out.println("Done!");
	}


    private void compare(ArrayList<Collection<Integer>> matC, ArrayList<ArrayList<Integer>> matAL,
            ArrayList<List<Integer>> matUL, ArrayList<int[]> matA, 
            ArrayList<PrimLongArrayList> matPL) {
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

        startTime("pList-f");
        for (PrimLongArrayList aList: matPL)
        for (int x = 0; x < N; x++) {
            for (Long b: aList) {
                n += b;
            }
        }
        stopTime("pList-f");

        startTime("pList-get(i)");
        for (PrimLongArrayList aList: matPL)
        for (int x = 0; x < N; x++) {
            for (int i = 0; i < aList.size(); i++) {
                n += aList.get(i);
            }
        }
        stopTime("pList-get(i)");

        startTime("pList-it");
        for (PrimLongArrayList aList: matPL)
        for (int x = 0; x < N; x++) {
            LongIterator aIt = aList.iterator(); 
            while (aIt.hasNext()) {
                n += aIt.next();
            }
        }
        stopTime("pList-it");
        
        startTime("pList-it2");
        for (PrimLongArrayList aList: matPL)
        for (int x = 0; x < N; x++) {
            LongIterator aIt = aList.iterator(); 
            while (aIt.hasNextLong()) {
                n += aIt.nextLong();
            }
        }
        stopTime("pList-it2");
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
