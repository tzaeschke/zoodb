/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.api;

import java.util.Arrays;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * Test class for database performance tests.
 *
 * @author Tilmann Zaeschke
 */
public class TestSuper extends PersistenceCapableImpl {

    private long _time;         //For indexing
    private long _id;           //For indexing
    private long[] _rawData;    //To give the object a
                                // specific size
    private TestSuper _child1 = null;
    private int _dummy = 0;     //This is only to make the size a multiple of 8

    private int[] largeInt;
    private byte[] largeByte;
    private String largeString;
    private Object[] largeObj;
    private TestSuper[] largePObj;
    private Long[] largeLObj;
    
    /**
     * 
     */
    public TestSuper() {
        //Only for Propagation
    }
    
   /**
     * @param time
     * @param id
     * @param data
     */
    public TestSuper(long time, long id, long[] data) {
        _time = time;
        _id = id;
        _rawData = data;
    }
    
    /**
     * @return data
     */
    public long[] getData() {
    	zooActivateRead();
        return _rawData;
    }

    /**
     * @param data
     */
    public void setData(long[] data) {
    	zooActivateWrite();
        _rawData = data;
    }

    /**
     * @return child.
     */
    public TestSuper getChild1() {
    	zooActivateRead();
        return _child1;
    }
    
    /**
     * @param pdChild
     */
    public void setChild1(TestSuper pdChild) {
    	zooActivateWrite();
        _child1 = pdChild;
    }
    
    /**
     * @return Time
     */
    public long getTime() {
    	zooActivateRead();
        return _time;
    }
    
    /**
     * @param time
     */
	public void setTime(long time) {
		zooActivateWrite();
		this._time = time;
	}

	/**
     * @return ID
     */
    public long getId() {
    	zooActivateRead();
        return _id;
    }

    /**
     * @param l
     */
    public void setId(long l) {
    	zooActivateWrite();
        _id = l;
    }
    
    @Override
    public boolean equals(Object o) {
    	zooActivateRead();
        if (o == null) return false;
        if (! (o instanceof TestSuper)) return false;
        if (this == o) {
        	return true;
        }
        TestSuper ts = (TestSuper) o;
        ts.zooActivateRead();
        if (_time != ts._time || _id != ts._id 
                || !Arrays.equals(_rawData, ts._rawData)
                || (_child1 == null ? ts._child1 != null : !_child1.equals(ts._child1))
                || _dummy != ts._dummy) {
            return false;
        }
        return true;
    }
    
    @Override
    public int hashCode() {
    	zooActivateRead();
    	int hash = 1;
    	hash = (int) (hash * 31 + _time);
    	hash = (int) (hash * 31 + _id);
    	hash = (int) (hash * 31 + _dummy);
    	hash = hash * 31 + (_child1 == null ? 0 : _child1.hashCode());
    	return hash;
    }
    
    public String toString() {
    	zooActivateRead();
        StringBuffer s = new StringBuffer();
        s.append("T=");
        s.append(_time);
        s.append("  ID=");
        s.append(_id);
        s.append("  RAW=");
        s.append(_rawData);
        s.append("  DUMMY=");
        s.append(_dummy);
        s.append("  CHILD=");
        s.append(_child1);
        return s.toString();
    }
    
    public void setLarge(int[] ia, byte[] ba, String str, Object[] oa, TestSuper[] ta, Long[] la) {
    	zooActivateWrite();
        largeByte = ba;
        largeInt = ia;
        largeString = str;
        largeObj = oa;
        largePObj = ta;
        largeLObj = la;
    }
    
    public int[] getLargeInt() {
    	zooActivateRead();
        return largeInt;
    }
    
    public byte[] getLargeByte() {
    	zooActivateRead();
        return largeByte;
    }
    
    public String getLargeStr() {
    	zooActivateRead();
        return largeString;
    }

    public Object[] getLargeObj() {
    	zooActivateRead();
        return largeObj;
    }
    
    public TestSuper[] getLargePersObj() {
    	zooActivateRead();
        return largePObj;
    }
    
    public Long[] getLargeLongObj() {
    	zooActivateRead();
        return largeLObj;
    }
}
