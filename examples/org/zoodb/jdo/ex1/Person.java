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
package org.zoodb.jdo.ex1;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.zoodb.api.impl.ZooPCImpl;

/**
 * Simple example for a persistent class.
 * 
 * @author ztilmann
 */
public class Person extends ZooPCImpl {

    private String name;
    private Set<Person> friends = new HashSet<>();
    
    @SuppressWarnings("unused")
    private Person() {
        // All persistent classes need a no-args constructor. 
        // The no-args constructor can be private.
    }
    
    public Person(String name) {
        // no activation required
        this.name = name;
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
    
    public void addFriend(Person p) {
        //activate and flag as dirty
        zooActivateWrite();
        this.friends.add(p);
    }
    
    public Collection<Person> getFriends() {
        //activate
        zooActivateRead();
        //prevent callers from modifying the set.
        return Collections.unmodifiableSet(friends);
	}
}
