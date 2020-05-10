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

import static org.junit.Assert.fail;

import java.util.Collections;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Transaction;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.testutil.TestTools;

/**
 * Smoke tests for examples and performance tests.
 * 
 * @author Tilmann Zaeschke
 */
public class Test_999_UnsupportedOperations {
    
    @BeforeClass
    public static void setUp() {
        TestTools.createDb();
        TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
    }

    @After
    public void afterTest() {
        TestTools.closePM();
    }
    
    @AfterClass
    public static void tearDown() {
        TestTools.closePM();
        TestTools.removeDb();
    }
    
    @Test
    public void testPMF() {
        ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        
        check(() -> pmf.addFetchGroups(null, null));
        check(() -> pmf.getConnectionDriverName());
        check(() -> pmf.getConnectionFactory());
        check(() -> pmf.getConnectionFactory2());
        check(() -> pmf.getConnectionFactory2Name());
        check(() -> pmf.getConnectionFactoryName());
        check(() -> pmf.getDataStoreCache());
        check(() -> pmf.getDatastoreReadTimeoutMillis());
        check(() -> pmf.getDatastoreWriteTimeoutMillis());
        check(() -> pmf.getFetchGroup(null, null));
        check(() -> pmf.getFetchGroups());
        check(() -> pmf.getManagedClasses());
        check(() -> pmf.getMapping());
        check(() -> pmf.getMetadata("ddd"));
        check(() -> pmf.getNontransactionalWrite());
        check(() -> pmf.getPersistenceManager("", ""));
        check(() -> pmf.getPersistenceManagerProxy());
        check(() -> pmf.getServerTimeZoneID());
        check(() -> pmf.getTransactionIsolationLevel());
        check(() -> pmf.getTransactionType());
        check(() -> pmf.registerMetadata(null));
        check(() -> pmf.removeAllFetchGroups());
        check(() -> pmf.removeFetchGroups(null, null));
        check(() -> pmf.setConnectionDriverName(null));
        check(() -> pmf.setConnectionFactory(""));
        check(() -> pmf.setConnectionFactory2(""));
        check(() -> pmf.setConnectionFactoryName(""));
        check(() -> pmf.setConnectionFactory2(""));
        check(() -> pmf.setCopyOnAttach(true));
        check(() -> pmf.setDatastoreReadTimeoutMillis(123));
        check(() -> pmf.setDatastoreWriteTimeoutMillis(123));
        check(() -> pmf.setMapping(""));
        check(() -> pmf.setNontransactionalWrite(true));
        check(() -> pmf.setServerTimeZoneID(""));
        check(() -> pmf.setTransactionIsolationLevel(""));
        check(() -> pmf.setTransactionType(""));
        check(() -> pmf.supportedOptions());
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void testPM() {
        PersistenceManager pm = TestTools.openPM();
        
        TestClass pc = new TestClass();
        
        check(() -> pm.detachCopy(pc));
        check(() -> pm.detachCopyAll(Collections.emptyList()));
        check(() -> pm.detachCopyAll(new TestClass[] {pc}));
        check(() -> pm.getCopyOnAttach());
        check(() -> pm.getFetchGroup(null, ""));
        //check(() -> pm.getFetchPlan());
        check(() -> pm.getSequence(""));
        check(() -> pm.getServerDate());
        check(() -> pm.getSupportedProperties());
        check(() -> pm.getUserObject());
        check(() -> pm.getUserObject(pc));
        check(() -> pm.makeNontransactional(pc));
        check(() -> pm.makeNontransactional(Collections.emptyList()));
        check(() -> pm.makeNontransactional(new TestClass[] {pc}));
        check(() -> pm.makePersistentAll(Collections.emptyList()));
        check(() -> pm.makePersistentAll(new TestClass[] {pc}));
        check(() -> pm.makeTransactional(pc));
        check(() -> pm.makeTransactionalAll(Collections.emptyList()));
        check(() -> pm.makeTransactionalAll(pc, pc));
        check(() -> pm.newInstance(TestClass.class));
        check(() -> pm.newNamedQuery(TestClass.class, ""));
        check(() -> pm.newObjectIdInstance(TestClass.class, 123));
        check(() -> pm.putUserObject(123, 456));
        check(() -> pm.removeUserObject(123));
        check(() -> pm.retrieve(pc));
        check(() -> pm.retrieve(pc, false));
        check(() -> pm.retrieveAll(Collections.emptyList()));
        check(() -> pm.retrieveAll(pc, pc));
        //check(() -> pm.retrieveAll(false, pc, pc));
        check(() -> pm.retrieveAll(Collections.emptyList(), false));
        check(() -> pm.retrieveAll(new TestClass[] {pc}, false));
        check(() -> pm.setCopyOnAttach(false));
        check(() -> pm.setDatastoreReadTimeoutMillis(123));
        check(() -> pm.setDatastoreWriteTimeoutMillis(123));
        check(() -> pm.setUserObject(pc));
    }
    
    @Test
    public void testTX() {
        PersistenceManager pm = TestTools.openPM();
        Transaction tx = pm.currentTransaction();
        
        check(() -> tx.setNontransactionalWrite(true));
        check(() -> tx.setOptimistic(true));
        check(() -> tx.setRollbackOnly());
        check(() -> tx.setSerializeRead(true));
    }
    
    
    private void check(Runnable r) {
        try {
            r.run();
            fail();
        } catch (UnsupportedOperationException e) {
            // Yep!
        }
    }
    }
