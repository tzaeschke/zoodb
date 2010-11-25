package org.zoodb.test.api;

import java.util.Arrays;

/**
 * Test class for database performance tests.
 *
 * @author Tilmann Zaeschke
 */
public class TestSuper {

    private long _time;         //For indexing
    private long _id;           //For indexing
    private long[] _rawData;    //To give the object a
                                // specific size
    private TestSuper _child1 = null;
    private int _dummy = 0;     //This is only to make the size a multiple of 8

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
        return _rawData;
    }

    /**
     * @param data
     */
    public void setData(long[] data) {
        _rawData = data;
    }

    /**
     * @return child.
     */
    public TestSuper getChild1() {
        return _child1;
    }
    
    /**
     * @param pdChild
     */
    public void setChild1(TestSuper pdChild) {
        _child1 = pdChild;
    }
    
    /**
     * @return Time
     */
    public long getTime() {
        return _time;
    }
    
    /**
     * @return ID
     */
    public long getId() {
        return _id;
    }

    /**
     * @param l
     */
    public void setId(long l) {
        _id = l;
    }
    
    public boolean equals(Object o) {
        if (o == null) return false;
        if (! (o instanceof TestSuper)) return false;
        TestSuper ts = (TestSuper) o;
        if (_time != ts._time || _id != ts._id 
                || !Arrays.equals(_rawData, ts._rawData)
                || _child1 == null ? ts._child1 != null : !_child1.equals(ts._child1)
                || _dummy != ts._dummy) {
            return false;
        }
        return true;
    }
    
    public String toString() {
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
        //TODO commented out because of Versant bug# 22389, which sometimes
        //causes the timestamp to increment w/o reason.
//        s.append("  TS=");
//        s.append(o_ts_timestamp);
        return s.toString();
    }
}
