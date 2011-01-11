package org.zoodb.test.java;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

public class PerfIterator {

    private static final int MAX_I = 10000000;
    
    @Test
    public void testIterator() {
        Collection<Byte> col = new ArrayList<Byte>();
        for (int i = 0; i < MAX_I; i++) {
            col.add((byte)i);
        }
        
        List<Byte> list = new ArrayList<Byte>(col);
        ArrayList<Byte> aList = new ArrayList<Byte>(col);
        
        //call sub-method, so hopefully the compiler does not recognize that these are all ArrayLists
        _useTimer = false;
        for (int i = 0; i < 10; i++) {
            compare(col, list, aList);
        }
        _useTimer = true;
        compare(col, list, aList);
    }
    
    private void compare(Collection<Byte> coll, List<Byte> list, ArrayList<Byte> aList) {
        int n = 0;
        startTime("coll-f");
        for (int x = 0; x < 10; x++) {
            for (Byte b: coll) {
                n += b;
            }
        }
        stopTime("coll-f");

        startTime("aList-f");
        for (int x = 0; x < 10; x++) {
            for (Byte b: aList) {
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
            Iterator<Byte> aIt = aList.iterator(); 
            while (aIt.hasNext()) {
                n += aIt.next();
            }
        }
        stopTime("coll-it");
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
