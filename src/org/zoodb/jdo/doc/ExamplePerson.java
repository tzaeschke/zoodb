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
package org.zoodb.jdo.doc;

import org.zoodb.jdo.spi.PersistenceCapableImpl;



/**
 * Simple example for a persistent class.
 * 
 * @author ztilmann
 */
public class ExamplePerson extends PersistenceCapableImpl {

    private String name;
    private ExampleAddress address;
    private int[] data;
    private int year = 1950;
    private ExampleCity[] cities;
    private ExampleInner ei = new ExampleInner();
    
    @SuppressWarnings("unused")
    private ExamplePerson() {
        // All persistent classes need a no-args constructor. 
        // The no-args constructor can be private.
    }
    
    public ExamplePerson(String name) {
        // no activation required
        this.name = name;
    }

    public void setName(String name) {
        //activate and flag as dirty
       activateWrite("name");
        this.name = name;
    }
    
    public String getName() {
        //activate
        activateRead("name");
        return this.name;
    }

	public ExampleAddress getAddress() {
		activateRead("address");
		return address;
	}

	public void setAddress(ExampleAddress address) {
		activateWrite("address");
		this.address = address;
	}

	public int[] getData() {
		activateRead("data");
		return data;
	}

	public void setData(int[] data) {
		activateWrite("data");
		this.data = data;
	}

	public ExampleCity[] getCities() {
		activateRead("cities");
		return cities;
	}

	public void setCities(ExampleCity[] cities) {
		activateWrite("cities");
		this.cities = cities;
	}
	
	public class ExampleInner {
		private int year = 1966;
		public ExampleInner() {
			
		}
	}
	
	
	
}