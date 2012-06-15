/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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

import org.zoodb.jdo.api.DBHashMap;
import org.zoodb.jdo.api.DBArrayList;
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
