package org.zoodb.test.api;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBVector;

/**
 * This dummy class has is implemented to allow testing of DBHashtable and
 * DBVector. This class is needed since the both containers only accept 
 * persistent capable classes as arguments in the constructor. The test class
 * itself cannot be made persistent because it extends a non-persistent super 
 * class from JUnit. 
 *
 * @author Tilmann Zaeschke
 */
public class PersistentDummyImpl { //TODO extends PersistenceCapableImpl {
    
	@SuppressWarnings("unused")
    private int o_ts_timestamp = 0;

    DBHashtable<?, ?> _dbHashtable = null;
    DBVector<?> _dbVector = null;
    byte[] _rawData = null;

    /**
     * @return data
     */
    public byte[] getData() {
        return _rawData;
    }

    /**
     * @param data
     */
    public void setData(byte[] data) {
        _rawData = data;
    }

	/**
	 * @return DBHashtable
	 */
	public DBHashtable<?,?> getDbHashtable() {
		return _dbHashtable;
	}

	/**
	 * @param hashtable
	 */
	public void setDbHashtable(DBHashtable<?,?> hashtable) {
		_dbHashtable = hashtable;
	}

	/**
	 * @return DBVector
	 */
	public DBVector<?> getDbVector() {
		return _dbVector;
	}

	/**
	 * @param vector
	 */
	public void setDbVector(DBVector<?> vector) {
		_dbVector = vector;
	}
}
