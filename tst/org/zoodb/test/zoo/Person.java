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
package org.zoodb.test.zoo;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.zoodb.api.impl.ZooPC;

/**
 * Simple example for a persistent class.
 * 
 * @author ztilmann
 */
public class Person extends ZooPC {

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
