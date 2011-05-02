package org.zoodb.test.java;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.zoodb.jdo.internal.server.PageAccessFileInMemory;
import org.zoodb.jdo.internal.server.index.PagedLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong;
import org.zoodb.jdo.internal.server.index.PagedUniqueLongLong.LLEntry;
import org.zoodb.jdo.stuff.PrimLongMapLI;

public class PerfIterator {

    //private static final int MAX_I = 2000000;
    private static final int MAX_I = 1000000;
    
	public static void main(String[] args) {
		new PerfIterator().run();
	}

    public void run() {
        ArrayList<Long> aList = new ArrayList<Long>(MAX_I);
        Map<Long, Long> map = new HashMap<Long, Long>(MAX_I);
        Map<Long, Long> mapId = new IdentityHashMap<Long, Long>(MAX_I);
        PrimLongMapLI<Long> lMap = new PrimLongMapLI<Long>(MAX_I);
        PagedUniqueLongLong ull = new PagedUniqueLongLong(new PageAccessFileInMemory());
        PagedLongLong ll = new PagedLongLong(new PageAccessFileInMemory());
        long[] array = new long[MAX_I];
        Long[] Array = new Long[MAX_I];
        for (int i = 0; i < MAX_I; i++) {
            aList.add((long)i);
            map.put((long)i, 0L);
            mapId.put((long)i, 0L);
            lMap.put((long)i, 0L);
            ull.insertLong(i, 0);
            ll.insertLong(i, 0);
            //array[i] = 0;
            Array[i] = Long.valueOf(0);
        }
        
        List<Long> list = aList;
        List<Long> uList = Collections.unmodifiableList(list);
        Collection<Long> coll = aList;
        HashMap<Long, Long> hMap = new HashMap<Long, Long>(map);
        
        
        //call sub-method, so hopefully the compiler does not recognize that these are all ArrayLists
        _useTimer = false;
        for (int i = 0; i < 3; i++) {
            compare(coll, list, aList, uList, array, Array);
        }
        _useTimer = true;
        compare(coll, list, aList, uList, array, Array);

        _useTimer = false;
        for (int i = 0; i < 3; i++) {
            compare(map, mapId, hMap, lMap, ull, ll);
        }
        _useTimer = true;
        compare(map, mapId, hMap, lMap, ull, ll);
    }
    
    private void compare(Collection<Long> coll, List<Long> list, ArrayList<Long> aList,
            List<Long> uList, long[] array, Long[] Array) {
        int n = 0;
        startTime("array");
        for (int x = 0; x < 10; x++) {
            for (long b: array) {
                n += b;
            }
        }
        stopTime("array");

        startTime("Array");
        for (int x = 0; x < 10; x++) {
            for (Long b: Array) {
                n += b;
            }
        }
        stopTime("Array");

        startTime("coll-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: coll) {
                n += b;
            }
        }
        stopTime("coll-f");

        startTime("uList-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: uList) {
                n += b;
            }
        }
        stopTime("uList-f");

        startTime("aList-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: aList) {
                n += b;
            }
        }
        stopTime("aList-f");

        startTime("aList-get(i)");
        for (int x = 0; x < 10; x++) {
            for (int i = 0; i < aList.size(); i++) {
                n += aList.get(i);
            }
        }
        stopTime("aList-get(i)");

        startTime("aList-it");
        for (int x = 0; x < 10; x++) {
            Iterator<Long> aIt = aList.iterator(); 
            while (aIt.hasNext()) {
                n += aIt.next();
            }
        }
        stopTime("aList-it");
    }
    
    private void compare(Map<Long, Long> map, Map<Long, Long> mapId, HashMap<Long, Long> hMap, 
    		PrimLongMapLI<Long> lMap, PagedUniqueLongLong ull, PagedLongLong ll) {
        int n = 0;
        startTime("map-keyset-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: map.keySet()) {
                n += b;
            }
        }
        stopTime("map-keyset-f");

        startTime("mapID-keyset-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: mapId.keySet()) {
                n += b;
            }
        }
        stopTime("mapID-keyset-f");

        startTime("hMap-keyset-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: hMap.keySet()) {
                n += b;
            }
        }
        stopTime("hMap-keyset-f");

        startTime("lMap-keyset-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: lMap.keySet()) {
                n += b;
            }
        }
        stopTime("lMap-keyset-f");

        startTime("lMap-keyset-it");
        for (int x = 0; x < 10; x++) {
            Iterator<Long> aIt = lMap.keySet().iterator(); 
            while (aIt.hasNext()) {
                n += aIt.next();
            }
        }
        stopTime("lMap-keyset-it");

        startTime("lMap-entry-f");
        for (int x = 0; x < 10; x++) {
            for (PrimLongMapLI.Entry<Long> e: lMap.entrySet()) {
                n += e.getKey();
            }
        }
        stopTime("lMap-entry-f");

        startTime("lMap-entry-it");
        for (int x = 0; x < 10; x++) {
            Iterator<PrimLongMapLI.Entry<Long>> aIt = lMap.entrySet().iterator(); 
            while (aIt.hasNext()) {
                n += aIt.next().getKey();
            }
        }
        stopTime("lMap-entry-it");

        startTime("ull-it");
        for (int x = 0; x < 10; x++) {
            Iterator<LLEntry> aIt = ull.iterator(Long.MIN_VALUE, Long.MAX_VALUE); 
            while (aIt.hasNext()) {
                n += aIt.next().getKey();
            }
        }
        stopTime("ull-it");

        startTime("ll-it");
        for (int x = 0; x < 10; x++) {
            Iterator<LLEntry> aIt = ll.iterator(Long.MIN_VALUE, Long.MAX_VALUE); 
            while (aIt.hasNext()) {
                n += aIt.next().getKey();
            }
        }
        stopTime("ll-it");
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
