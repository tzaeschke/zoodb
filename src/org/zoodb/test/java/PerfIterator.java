package org.zoodb.test.java;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.zoodb.jdo.stuff.PrimLongMapLI;

public class PerfIterator {

    //private static final int MAX_I = 5000000;
    private static final int MAX_I = 100000;
    
    @Test
    public void testIterator() {
        Collection<Long> col = new ArrayList<Long>();
        Map<Long, Long> map = new HashMap<Long, Long>();
        PrimLongMapLI<Long> lMap = new PrimLongMapLI<Long>();
        for (int i = 0; i < MAX_I; i++) {
            col.add((long)i);
            map.put((long)i, 0L);
            lMap.put((long)i, 0L);
        }
        
        List<Long> list = new ArrayList<Long>(col);
        ArrayList<Long> aList = new ArrayList<Long>(col);
        HashMap<Long, Long> hMap = new HashMap<Long, Long>(map);
        
        
        
        //call sub-method, so hopefully the compiler does not recognize that these are all ArrayLists
        _useTimer = false;
        for (int i = 0; i < 3; i++) {
            compare(col, list, aList);
        }
        _useTimer = true;
        compare(col, list, aList);

        _useTimer = false;
        for (int i = 0; i < 3; i++) {
            compare(map, hMap, lMap);
        }
        _useTimer = true;
        compare(map, hMap, lMap);
    }
    
    private void compare(Collection<Long> coll, List<Long> list, ArrayList<Long> aList) {
        int n = 0;
        startTime("coll-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: coll) {
                n += b;
            }
        }
        stopTime("coll-f");

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
        stopTime("coll-it");
    }
    
    private void compare(Map<Long, Long> map, HashMap<Long, Long> hMap, PrimLongMapLI<Long> lMap) {
        int n = 0;
        startTime("map-keyset-f");
        for (int x = 0; x < 10; x++) {
            for (Long b: map.keySet()) {
                n += b;
            }
        }
        stopTime("map-keyset-f");

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
