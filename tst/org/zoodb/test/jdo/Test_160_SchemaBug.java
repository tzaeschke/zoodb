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
package org.zoodb.test.jdo;
 
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooSchema;
import org.zoodb.test.testutil.TestTools;
 
public class Test_160_SchemaBug {
    
    @BeforeClass
    public static void setUp() {
        TestTools.createDb();
        TestTools.defineSchema(TestClass.class);
    }
 
    @After
    public void afterTest() {
        TestTools.closePM();
    }
    
    @AfterClass
    public static void tearDown() {
        TestTools.removeDb();
    }
 
    /**
     * Issue #77: Adding an attribute causes NPE in subsequent commit() of Java instances.
     */
    @Test
    public void testNpeAfterSchemaChange_Issue77() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        ZooSchema s = ZooJdoHelper.schema(pm);
        ZooClass c = s.getClass(TestClass.class);
        c.addField("test1", Integer.TYPE);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
 
        pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
        TestClass tc1 = new TestClass();
        TestClass tc1b = new TestClass();
        pm.makePersistent(tc1);
        pm.makePersistent(tc1b);
        tc1.setInt(5);
        tc1.setRef2(tc1b);
        tc1b.setInt(6);
 
        pm.currentTransaction().commit();
        TestTools.closePM();
    }
   
}
 
