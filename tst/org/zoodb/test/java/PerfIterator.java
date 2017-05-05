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
package org.zoodb.test.java;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.PagedLongLong;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.internal.util.BucketStack;
import org.zoodb.internal.util.BucketTreeStack;
import org.zoodb.internal.util.CritBit64;
import org.zoodb.internal.util.CritBit64.CBIterator;
import org.zoodb.internal.util.PrimLongArrayList;
import org.zoodb.internal.util.PrimLongMap;
import org.zoodb.internal.util.PrimLongMap.PrimLongEntry;
import org.zoodb.internal.util.PrimLongMapLI;
import org.zoodb.internal.util.PrimLongMapZ;
import org.zoodb.tools.ZooConfig;


/**
 * Testing collection performance.
 * 
 * ArrayList vs BucketStack:
 * 100.000 entries: ArrayList is twice as fast for insert/remove
 * 1.000.000 entries: BucketsStack is twice faster on insert, ArrayList is twice faster on remove
 * and 5 times faster for iteration. 
 * 
 * @author Tilmann Zaeschke
 */
public class PerfIterator {

	//private static final int MAX_I = 2000000;
	private static final int MAX_I = 100000;
	private static final int N = 100;
	private static final int NM = 100; //maps

	public static void main(String[] args) {
		new PerfIterator().run();
	}

	public void run() {
		ArrayList<Long> aList = new ArrayList<Long>();//MAX_I);
		LinkedList<Long> lList = new LinkedList<Long>();//MAX_I);
		PrimLongArrayList plList = new PrimLongArrayList();
		Map<Long, Long> map = new HashMap<Long, Long>(MAX_I);
		Map<Long, Long> mapId = new IdentityHashMap<Long, Long>(MAX_I);
		//Map<Long, Long> mapId = new TreeMap<Long, Long>();
		PrimLongMapLI<Long> lMap = new PrimLongMapLI<Long>(MAX_I);
		PrimLongMapZ<Long> lMapZ = new PrimLongMapZ<Long>(MAX_I);
		PagedUniqueLongLong ull = new PagedUniqueLongLong(PAGE_TYPE.GENERIC_INDEX, 
					new StorageRootInMemory(ZooConfig.getFilePageSize()));
		PagedLongLong ll = new PagedLongLong(PAGE_TYPE.GENERIC_INDEX, 
					new StorageRootInMemory(ZooConfig.getFilePageSize()));
		BucketTreeStack<Long> bal = new BucketTreeStack<Long>((byte) 10);
		BucketStack<Long> bs = new BucketStack<Long>(1000);
		long[] array = new long[MAX_I];
		Long[] Array = new Long[MAX_I];
		CritBit64<Long> cb = CritBit64.create();
		for (int i = 0; i < MAX_I; i++) {
			//            aList.add((long)i);
			map.put((long)i, 0L);
			mapId.put((long)i, 0L);
			lMap.put((long)i, 0L);
			lMapZ.put((long)i, 0L);
			ull.insertLong(i, 0);
			ll.insertLong(i, 0);
			//            bal.add(0L);
			//            bs.push(0L);
			//array[i] = 0;
			//            Array[i] = Long.valueOf(0);
			cb.put(i, 0L);
		}

		List<Long> list = aList;
		List<Long> uList = Collections.unmodifiableList(list);
		Collection<Long> coll = aList;
//		HashMap<Long, Long> hMap = new HashMap<Long, Long>(map);

				_useTimer = true;
				compareInsert(aList, lList, array, Array, bal, bs, plList);
				compareRemove(aList, lList, array, Array, bal, bs, plList);
				compareInsert(aList, lList, array, Array, bal, bs, plList);
		
		
//				//call sub-method, so hopefully the compiler does not recognize that these are all ArrayLists
//				_useTimer = false;
//				for (int i = 0; i < 3; i++) {
//					compare(coll, list, aList, uList, array, Array, bal, bs);
//				}
//				_useTimer = true;
//				compare(coll, list, aList, uList, array, Array, bal, bs);
//				compare(coll, list, aList, uList, array, Array, bal, bs);
//				compare(coll, list, aList, uList, array, Array, bal, bs);

		_useTimer = false;
		for (int i = 0; i < 3; i++) {
			compare(map, mapId, (HashMap<Long, Long>)map, lMap, lMapZ, ull, ll, cb);
		}
		_useTimer = true;
		System.gc();
		System.gc();
		System.gc();
		compare(map, mapId, (HashMap<Long, Long>)map, lMap, lMapZ, ull, ll, cb);

		System.out.println("Done!");
	}


	private void compareInsert(ArrayList<Long> aList,
			List<Long> lList, long[] array, Long[] Array, BucketTreeStack<Long> bal, 
			BucketStack<Long> bs, PrimLongArrayList plList) {

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

		startTime("PL-i");
		for (int x = 0; x < N; x++) 
			for (int i = 0; i < MAX_I; i++) {
				plList.add(1L);
			}
		stopTime("PL-i");
	}


	private void compareRemove(ArrayList<Long> aList,
			List<Long> lList, long[] array, Long[] Array, BucketTreeStack<Long> bal, 
			BucketStack<Long> bs, PrimLongArrayList plList) {

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

		startTime("PL-r");
		for (int x = 0; x < N; x++) 
			for (int i = MAX_I - 1; i >= 0; i--) {
				plList.removePos(plList.size()-1);
			}
		stopTime("PL-r");
	}


	private void compare(Collection<Long> coll, List<Long> list, ArrayList<Long> aList,
			List<Long> uList, long[] array, Long[] Array, BucketTreeStack<Long> bal, 
			BucketStack<Long> bs) {
		int n = 0;
		startTime("array");
		for (int x = 0; x < N; x++) {
			for (long b: array) {
				n += b;
			}
		}
		stopTime("array");

		startTime("Array");
		for (int x = 0; x < N; x++) {
			for (Long b: Array) {
				n += b;
			}
		}
		stopTime("Array");

		startTime("coll-f");
		for (int x = 0; x < N; x++) {
			for (Long b: coll) {
				n += b;
			}
		}
		stopTime("coll-f");

		startTime("uList-f");
		for (int x = 0; x < N; x++) {
			for (Long b: uList) {
				n += b;
			}
		}
		stopTime("uList-f");

		startTime("aList-f");
		for (int x = 0; x < N; x++) {
			for (Long b: aList) {
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
			Iterator<Long> aIt = aList.iterator(); 
			while (aIt.hasNext()) {
				n += aIt.next();
			}
		}
		stopTime("aList-it");

		startTime("BAL-get(i)");
		for (int x = 0; x < N; x++) {
			for (int i = 0; i < bal.size(); i++) {
				n += bal.get(i);
			}
		}
		stopTime("BAL-get(i)");

		startTime("BAL-it");
		for (int x = 0; x < N; x++) {
			Iterator<Long> aIt = bal.iterator(); 
			while (aIt.hasNext()) {
				n += aIt.next();
			}
		}
		stopTime("BAL-it");

		startTime("BS-it");
		for (int x = 0; x < N; x++) {
			Iterator<Long> aIt = bs.iterator(); 
			while (aIt.hasNext()) {
				n += aIt.next();
			}
		}
		stopTime("BS-it");

		startTime("BS-f");
		for (int x = 0; x < N; x++) {
			for (Long b: bs) {
				n += b;
			}
		}
		stopTime("BS-f");
		
		//ensure that n is not optimized away
		if (n == 0) {
			throw new IllegalStateException();
		}
	}

	private void compare(Map<Long, Long> map, Map<Long, Long> mapId, HashMap<Long, Long> hMap, 
			PrimLongMapLI<Long> lMap, PrimLongMapZ<Long> lMapZ, 
			PagedUniqueLongLong ull, PagedLongLong ll,
			CritBit64<Long> cb) {
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
			for (PrimLongEntry<Long> e: lMap.entrySet()) {
				n += e.getKey();
			}
		}
		stopTime("lMap-entry-f");

		startTime("lMap-entry-it");
		for (int x = 0; x < NM; x++) {
			Iterator<PrimLongMap.PrimLongEntry<Long>> aIt = lMap.entrySet().iterator(); 
			while (aIt.hasNext()) {
				n += aIt.next().getKey();
			}
		}
		stopTime("lMap-entry-it");

		startTime("lMapZ-keyset-f");
		for (int x = 0; x < NM; x++) {
			for (Long b: lMapZ.keySet()) {
				n += b;
			}
		}
		stopTime("lMapZ-keyset-f");

		startTime("lMapZ-keyset-it");
		for (int x = 0; x < NM; x++) {
			Iterator<Long> aIt = lMapZ.keySet().iterator(); 
			while (aIt.hasNext()) {
				n += aIt.next();
			}
		}
		stopTime("lMapZ-keyset-it");

		startTime("lMapZ-entry-f");
		for (int x = 0; x < NM; x++) {
			for (PrimLongEntry<Long> e: lMapZ.entrySet()) {
				n += e.getKey();
			}
		}
		stopTime("lMapZ-entry-f");

		startTime("lMapZ-entry-it");
		for (int x = 0; x < NM; x++) {
			Iterator<PrimLongMap.PrimLongEntry<Long>> aIt = lMapZ.entrySet().iterator(); 
			while (aIt.hasNext()) {
				n += aIt.next().getKey();
			}
		}
		stopTime("lMapZ-entry-it");

		startTime("ull-it");
		for (int x = 0; x < NM; x++) {
			Iterator<LongLongIndex.LLEntry> aIt = ull.iterator(Long.MIN_VALUE, Long.MAX_VALUE); 
			while (aIt.hasNext()) {
				n += aIt.next().getKey();
			}
		}
		stopTime("ull-it");

		startTime("ll-it");
		for (int x = 0; x < NM; x++) {
			Iterator<LongLongIndex.LLEntry> aIt = ll.iterator(Long.MIN_VALUE, Long.MAX_VALUE); 
			while (aIt.hasNext()) {
				n += aIt.next().getKey();
			}
		}
		stopTime("ll-it");
		
		startTime("cb-it");
		for (int x = 0; x < NM; x++) {
			CBIterator<Long> aIt = cb.iterator(); 
			while (aIt.hasNext()) {
				n += aIt.nextKey();
			}
		}
		stopTime("cb-it");

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
