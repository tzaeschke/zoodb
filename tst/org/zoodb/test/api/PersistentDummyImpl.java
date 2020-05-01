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

import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBHashMap;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * This dummy class has is implemented to allow testing of DBHashtable and
 * DBVector. This class is needed since the both containers only accept 
 * persistent capable classes as arguments in the constructor. The test class
 * itself cannot be made persistent because it extends a non-persistent super 
 * class from JUnit. 
 *
 * @author Tilmann Zaeschke
 */
public class PersistentDummyImpl extends PersistenceCapableImpl {

    @SuppressWarnings("unused")
    private int o_ts_timestamp = 0;

    private DBHashMap<?, ?> _dbHashtable = null;
    private DBArrayList<?> _dbVector = null;
    private byte[] _rawData = null;

    /**
     * @return data
     */
    public byte[] getData() {
        zooActivateRead();
        return _rawData;
    }

    /**
     * @param data
     */
    public void setData(byte[] data) {
        zooActivateWrite();
        _rawData = data;
    }

    /**
     * @return DBHashtable
     */
    public DBHashMap<?,?> getDbHashtable() {
        zooActivateRead();
        return _dbHashtable;
    }

    /**
     * @param hashtable
     */
    public void setDbHashtable(DBHashMap<?,?> hashtable) {
        zooActivateWrite();
        _dbHashtable = hashtable;
    }

    /**
     * @return DBVector
     */
    public DBArrayList<?> getDbVector() {
        zooActivateRead();
        return _dbVector;
    }

    /**
     * @param vector
     */
    public void setDbVector(DBArrayList<?> vector) {
        zooActivateWrite();
        _dbVector = vector;
    }
}
