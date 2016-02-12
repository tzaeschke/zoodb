/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.tools.ZooHelper;

/**
 * Simple example that creates a database, writes an object to it and then reads the object.
 * 
 * @author ztilmann
 */
public class ExampleMain {
    
    
    public static void main(String[] args) {
        String dbName = "ExampleDB.zdb";
        createDB(dbName);
        populateDB(dbName);
        readDB(dbName);
    }
    
    
    /**
     * Read data from a database.
     * Extents are fast, but allow filtering only on the class.
     * Queries are a bit more powerful than Extents.
     *  
     * @param dbName Database name.
     */
    @SuppressWarnings("unchecked")
	private static void readDB(String dbName) {
        PersistenceManager pm = ZooJdoHelper.openDB(dbName);
        pm.currentTransaction().begin();

        //Extents are one way to get objects from a database:
        System.out.println("Person extent: ");
        Extent<Person> ext = pm.getExtent(Person.class);
        for (Person p: ext) {
            System.out.println("Person found: " + p.getName());
        }
        ext.closeAll();
        
        //Queries are more powerful:
        System.out.println("Queries: ");
        Query query = pm.newQuery(Person.class, "name == 'Bart'");
        Collection<Person> barts = (Collection<Person>) query.execute();
        for (Person p: barts) {
            System.out.println("Person found called 'Bart': " + p.getName());
        }
        query.closeAll();
        
        //Once an object is loaded, normal method calls can be used to traverse the object graph.
        Person bart = barts.iterator().next();
        System.out.println(bart.getName() + " has " + bart.getFriends().size() + " friend(s):");
        for (Person p: bart.getFriends()) {
            System.out.println(p.getName() + " is a friend of " + bart.getName());
        }
        
        
        pm.currentTransaction().commit();
        closeDB(pm);
    }
    
    
    /**
     * Populate a database.
     * 
     * ZooDB supports persistence by reachability. This means that if 'lisa' is stored in the
     * database, 'bart' will also be stored because it is referenced from 'lisa'.
     * The zooActivate(...) methods in {@code Person.addFriend()} ensure that 'bart' is flagged as modified
     * when {@code addFriend()} is called, so in the second part an updated 'bart' and 'maggie'
     * will be stored.
     * 
     * @param dbName Database name.
     */
    private static void populateDB(String dbName) {
        PersistenceManager pm = ZooJdoHelper.openDB(dbName);
        pm.currentTransaction().begin();
        
        // create instances
        Person lisa = new Person("Lisa");
        //make Lisa persistent. 
        pm.makePersistent(lisa);

        //add Bart to Lisa's friends
        Person bart = new Person("Bart");
        lisa.addFriend(bart);
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        bart.addFriend(new Person("Maggie"));
        
        pm.currentTransaction().commit();
        closeDB(pm);
    }

    
    /**
     * Create a database.
     * 
     * @param dbName Name of the database to create.
     */
    private static void createDB(String dbName) {
        // remove database if it exists
        if (ZooHelper.dbExists(dbName)) {
            ZooHelper.removeDb(dbName);
        }

        // create database
        // By default, all database files will be created in %USER_HOME%/zoodb
        ZooHelper.createDb(dbName);
    }

    
    /**
     * Close the database connection.
     * 
     * @param pm The current PersistenceManager.
     */
    private static void closeDB(PersistenceManager pm) {
        if (pm.currentTransaction().isActive()) {
            pm.currentTransaction().rollback();
        }
        pm.close();
        pm.getPersistenceManagerFactory().close();
    }
       
}


