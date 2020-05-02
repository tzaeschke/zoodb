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
     * @param time time
     * @param id id
     * @param data data
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
     * @param data data
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
     * @param pdChild child
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
     * @param time time
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
     * @param l number
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
    	hash = hash * 31 + _dummy;
    	hash = hash * 31 + (_child1 == null ? 0 : _child1.hashCode());
    	return hash;
    }
    
    @Override
	public String toString() {
    	zooActivateRead();
        StringBuffer s = new StringBuffer();
        s.append("T=");
        s.append(_time);
        s.append("  ID=");
        s.append(_id);
        s.append("  RAW=");
        s.append(Arrays.toString(_rawData));
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
