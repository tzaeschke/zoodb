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

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.util.TransientField;

/**
 * Test class for database performance tests.
 *
 * @author Tilmann Zaeschke
 */
public class TestTransient extends ZooPCImpl implements Cloneable {

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
     * @param pdChild
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
     * @param l
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
	 * @param b
	 */
	public void setTb1(boolean b) {
		_tb1.set(this, b);
	}

	/**
	 * @param b
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
	 * @param tb2
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
	 * @param tb2
	 */
	public void setTb2(Boolean tb2) {
		_tb2.set(this, tb2);
	}

	/**
	 * @param tb3
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
	 * @param tb3
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
	 * @param to1
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
	 * @param to2
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
     * @param to3
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
