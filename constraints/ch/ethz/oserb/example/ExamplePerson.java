/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
package ch.ethz.oserb.example;

import org.zoodb.api.impl.ZooPCImpl;

import net.sf.oval.constraint.Assert;
import net.sf.oval.context.OValContext;

/**
 * Simple example for a persistent class.
 * 
 * @author oserb
 */
//@Assert(expr="context ExamplePerson inv: self.age>0", lang="ocl")
public class ExamplePerson extends ZooPCImpl {
	
    private String name;
	
    /*@Assert(expr="context ExamplePerson inv: self.age>0", lang="ocl")*/
	private int age;
    
    @SuppressWarnings("unused")
    private ExamplePerson() {
        // All persistent classes need a no-args constructor. 
        // The no-args constructor can be private.
    }
    
    public ExamplePerson(String name) {
        // no activation required
        this.name = name;
    }
    public ExamplePerson(String name, int age) {
        // no activation required
        this.name = name;
        this.age = age;
    }

    public void setName(String name) {
        //activate and flag as dirty
        zooActivateWrite();
        this.name = name;
    }
    
    public String getName() {
        //activate
        zooActivateRead();
        return this.name;
    }
    public void setAge(int age){
    	zooActivateWrite();
    	this.age = age;
    }
    public int getAge(){
    	zooActivateRead();
    	return this.age;
    }
}
