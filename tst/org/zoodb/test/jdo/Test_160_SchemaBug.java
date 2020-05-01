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
package org.zoodb.test.jdo;
 
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.internal.util.DBLogger;
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
        try {
        	pm.currentTransaction().commit();
        	fail();
        } catch (RuntimeException e) {
        	assertTrue(DBLogger.isUser(e));
        	assertTrue(e.getMessage().contains("Schema"));
        }
        TestTools.closePM();
    }
   
}
 
