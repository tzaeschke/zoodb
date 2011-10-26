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

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooHelper;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;

/**
 * Simple example that create a database, writes an object and reads it.
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
    
    private static void readDB(String dbName) {
        PersistenceManager pm = openDB(dbName);
        pm.currentTransaction().begin();

        Extent<ExamplePerson> ext = pm.getExtent(ExamplePerson.class);
        ExamplePerson p = ext.iterator().next();
        ext.closeAll();
        
        System.out.println("Person found: " + p.getName());
        
        pm.currentTransaction().commit();
        closeDB(pm);
    }
    
    
    private static void populateDB(String dbName) {
        PersistenceManager pm = openDB(dbName);
        pm.currentTransaction().begin();
        
        // define schema
        ZooSchema.create(pm, ExamplePerson.class);
        
        // create instance
        pm.makePersistent(new ExamplePerson("Fred"));
        
        pm.currentTransaction().commit();
        closeDB(pm);
    }

    
    private static void createDB(String dbName) {
        // one repository for all databases
        // located at %USER_HOME%/zoodb
        if (!ZooHelper.getDataStoreManager().repositoryExists()) {
            ZooHelper.getDataStoreManager().createDbRepository();
        }

        // remove database if it exists
        if (ZooHelper.getDataStoreManager().dbExists(dbName)) {
            ZooHelper.getDataStoreManager().removeDb(dbName);
        }
        // create database
        ZooHelper.getDataStoreManager().createDb(dbName);
    }

    private static PersistenceManager openDB(String dbName) {
        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        return pm;
    }
    
    private static void closeDB(PersistenceManager pm) {
        if (pm.currentTransaction().isActive()) {
            pm.currentTransaction().rollback();
        }
        pm.close();
        pm.getPersistenceManagerFactory().close();
    }
    
    
}


