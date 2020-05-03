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

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.util.TransientField;

/**
 * Test class for database performance tests.
 *
 * @author Tilmann Zaeschke
 */
public class TestTransient extends ZooPC implements Cloneable {

    private long _time = 0;         //For indexing
    private long _id = 0;           //For indexing
    TestTransient _child1 = null;

    private static TransientField<Object> _to1 = 
        new TransientField<Object>(null);     
    private static TransientField<Object> _to2 = 
        new TransientField<Object>("fdfd");     
    private static TransientField<Object> _to3 = 
        new TransientField<Object>(null);     
    private static TransientField<Boolean> _tb1 = 
        new TransientField<Boolean>(true);    
    private static TransientField<Boolean> _tb2 = 
        new TransientField<Boolean>(null);     
    private static TransientField<Boolean> _tb3 = 
        new TransientField<Boolean>(Boolean.TRUE);     

    /**
     */
    public TestTransient() {
    	//
    }
    
    /**
     * @param pdChild child
     */
    public void setChild1(TestTransient pdChild) {
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
     * @return ID
     */
    public long getId() {
        zooActivateRead();
        return _id;
    }

    /**
     * @param l long
     */
    public void setId(long l) {
        zooActivateWrite();
        _id = l;
    }

	/**
	 * @return boolean
	 */
	public boolean getTb1() {
		return _tb1.get(this);
	}

	/**
	 * @param b boolean
	 */
	public void setTb1(boolean b) {
		_tb1.set(this, b);
	}

	/**
	 * @param b boolean
	 */
	public void setTb1F(Boolean b) {
		_tb1.set(this, b);
	}

	/**
	 * @return boolean
	 */
	public boolean getTb2F() {
		return _tb2.get(this);
	}

	/**
	 * @param tb2 boolean
	 */
	public void setTb2F(boolean tb2) {
		_tb2.set(this, tb2);
	}

	/**
	 * @return boolean
	 */
	public Boolean getTb2() {
		return _tb2.get(this);
	}

	/**
	 * @param tb2 boolean
	 */
	public void setTb2(Boolean tb2) {
		_tb2.set(this, tb2);
	}

	/**
	 * @param tb3 boolean
	 */
	public void setTb3F(boolean tb3) {
		_tb3.set(this, tb3);
	}

	/**
	 * @return boolean
	 */
	public Boolean getTb3() {
		return _tb3.get(this);
	}

	/**
	 * @param tb3 boolean
	 */
	public void setTb3(Boolean tb3) {
		_tb3.set(this, tb3);
	}

	/**
	 * @return Object
	 */
	public Object getTo1() {
		return _to1.get(this);
	}

	/**
	 * @param to1 boolean
	 */
	public void setTo1(Object to1) {
		_to1.set(this, to1);
	}

	/**
	 * @return Object
	 */
	public Object getTo2() {
		return _to2.get(this);
	}

	/**
	 * @param to2 Obejct
	 */
	public void setTo2(Object to2) {
		_to2.set(this, to2);
	}
	
    /**
     * @return Object
     */
    public Object getTo3() {
        return _to3.get(this);
    }

    /**
     * @param to3 Obejct
     */
    public void setTo3(Object to3) {
        _to2.set(this, to3);
    }
    
	/**
	 * Unregister.
	 */
	public void deregister() {
		_tb1.deregisterOwner(this);
		_tb2.deregisterOwner(this);
		_tb3.deregisterOwner(this);
		_to1.deregisterOwner(this);
		_to2.deregisterOwner(this);
	}
    
    @Override
    public TestTransient clone() {
        zooActivateRead();
        TestTransient obj;
        try {
            obj = (TestTransient)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        return obj;
    }

    /**
     * @return TransientField
     */
    public static TransientField<Object> getTfTo1() {
        return _to1;
    }

    /**
     * @return TransientField
     */
    public static TransientField<Object> getTfTo2() {
        return _to2;
    }

    /**
     * @return TransientField
     */
    public static TransientField<Boolean> getTfTb1() {
        return _tb1;
    }
    
    @Override
    protected void finalize() throws Throwable {
        try {
            _tb1.cleanIfTransient(this);
            _tb2.cleanIfTransient(this);
            _tb3.cleanIfTransient(this);
            _to1.cleanIfTransient(this);
            _to2.cleanIfTransient(this);
            _to3.cleanIfTransient(this);
        } finally {
            super.finalize();
        }
    }
}
