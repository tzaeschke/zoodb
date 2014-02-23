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

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.zoodb.jdo.api.ZooJdoHelper;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.tools.ZooHelper;

/**
 * Simple example that creates a database, writes an object to it and then reads the object.
 * 
 * @author ztilmann
 */
public class Example {
    
    
    public static void main(String[] args) {
        String dbName = "ExampleDB";
        createDB(dbName);
        populateDB(dbName);
        readDB(dbName);
    }
    
    
    /**
     * Read data from a database.
     *  
     * @param dbName Database name.
     */
    private static void readDB(String dbName) {
        PersistenceManager pm = ZooJdoHelper.openDB(dbName);
        pm.currentTransaction().begin();

        Extent<ExamplePerson> ext = pm.getExtent(ExamplePerson.class);
        ExamplePerson p = ext.iterator().next();
        ext.closeAll();
        
        System.out.println("Person found: " + p.getName());
        
        pm.currentTransaction().commit();
        closeDB(pm);
    }
    
    
    /**
     * Populate a database.
     * 
     * @param dbName Database name.
     */
    private static void populateDB(String dbName) {
        PersistenceManager pm = ZooJdoHelper.openDB(dbName);
        pm.currentTransaction().begin();
        
        // define schema
        ZooSchema.defineClass(pm, ExamplePerson.class);
        
        // create instance
        pm.makePersistent(new ExamplePerson("Fred"));
        
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


