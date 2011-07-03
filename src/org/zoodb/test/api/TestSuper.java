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
    
    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (! (o instanceof TestSuper)) return false;
        if (this == o) {
        	return true;
        }
        TestSuper ts = (TestSuper) o;
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
    	int hash = 1;
    	hash = (int) (hash * 31 + _time);
    	hash = (int) (hash * 31 + _id);
    	hash = (int) (hash * 31 + _dummy);
    	hash = hash * 31 + (_child1 == null ? 0 : _child1.hashCode());
    	return hash;
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
        return s.toString();
    }
    
    public void setLarge(int[] ia, byte[] ba, String str, Object[] oa, TestSuper[] ta, Long[] la) {
        largeByte = ba;
        largeInt = ia;
        largeString = str;
        largeObj = oa;
        largePObj = ta;
        largeLObj = la;
    }
    
    public int[] getLargeInt() {
        return largeInt;
    }
    
    public byte[] getLargeByte() {
        return largeByte;
    }
    
    public String getLargeStr() {
        return largeString;
    }

    public Object[] getLargeObj() {
        return largeObj;
    }
    
    public TestSuper[] getLargePersObj() {
        return largePObj;
    }
    
    public Long[] getLargeLongObj() {
        return largeLObj;
    }
}
