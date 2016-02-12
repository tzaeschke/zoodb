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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.api.DBLargeVector;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooCompareDb;

public class Test_015_DatabaseComparison {

	private static final String DB2 = TestTools.getDbName() + "2";
	
    @Before
    public void before() {
        TestTools.createDb();
        TestTools.createDb(DB2);
    }
    
    @After
    public void after() {
        TestTools.closePM();
        TestTools.removeDb();
        TestTools.removeDb(DB2);
        removeFile( FileSystems.getDefault().getPath(TestTools.getDbFileName() + "2") );
        removeFile( FileSystems.getDefault().getPath(TestTools.getDbFileName()) );
    }
    
    private void removeFile(Path p) {
        if (Files.exists(p)) {
        	try {
				Files.delete(p);
			} catch (IOException e) {
	        	throw new RuntimeException(e);
			}
        }
    }
    
    private void copyDB() {
        try {
	        Path p1 = FileSystems.getDefault().getPath(TestTools.getDbFileName());
	        Path p2 = FileSystems.getDefault().getPath(TestTools.getDbFileName() + "2");
	        removeFile(p2);
	        Files.copy(p1, p2);
        } catch (IOException e) {
        	throw new RuntimeException(e);
        }
    }
    
    @Test
    public void testEmptyDB() {
        copyDB();

        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertEquals("", result);
    }
    
	private void populateSimple() {
		TestTools.defineSchema(TestClass.class);
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'e', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)127, (short)32000, 1234567890L, "xyz", new byte[]{1,2},
        		11.1f, -35);
        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        TestTools.closePM();;
	}

	
    @Test
    public void testSimpleClasses() {
    	//populate
    	populateSimple();

        copyDB();

        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertEquals("", result);
    }

    private void populateComplex() {
       	TestTools.defineSchema(TestSerializer.class, TestSuper.class, DBLargeVector.class);
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        TestSerializer ts1 = new TestSerializer();
        ts1.init();
        pm.makePersistent(ts1);
        pm.currentTransaction().commit();
        TestTools.closePM();
    }

    @Test
    public void testComplexClassCopy() {
    	//populate
        populateComplex();
        copyDB();
    
        //This causes the serialization tests to fail!
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertEquals("", result);
    }
    
    @Test
    public void testSimpleFailsClasses1() {
    	populateSimple();
        copyDB();

        TestTools.defineSchema(TestSerializer.class, TestSuper.class, DBLargeVector.class);
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains(TestSerializer.class.getName()));
    }

    @Test
    public void testSimpleFailsClasses2() {
    	populateSimple();
        copyDB();

        TestTools.defineSchema(DB2, TestSerializer.class, TestSuper.class, DBLargeVector.class);
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains(TestSerializer.class.getName()));
    }

    @Test
    public void testSimpleFailsFieldName1() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        ZooJdoHelper.schema(pm).getClass(TestClass.class).addField("dummy", Integer.TYPE);
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("dummy"));
    }

    @Test
    public void testSimpleFailsFieldName2() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM(DB2);
        pm.currentTransaction().begin();
        ZooJdoHelper.schema(pm).getClass(TestClass.class).addField("dummy", Integer.TYPE);
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("dummy"));
    }

    @Test
    public void testSimpleFailsFieldType() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        ZooJdoHelper.schema(pm).getClass(TestClass.class).addField("dummy", Integer.TYPE);
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        PersistenceManager pm2 = TestTools.openPM(DB2);
        pm2.currentTransaction().begin();
        ZooJdoHelper.schema(pm2).getClass(TestClass.class).addField("dummy", Integer.class);
        pm2.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("dummy"));
    }

    @Test
    public void testSimpleFailsFieldValue() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        Query q = pm.newQuery(TestClass.class, "_int == 12");
        q.setUnique(true);
        TestClass t1 = (TestClass) q.execute();
        t1.setInt(23);
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("_int"));
    }

    @Test
    public void testSimpleFailsFieldValueArray() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        Query q = pm.newQuery(TestClass.class, "_int == 12");
        q.setUnique(true);
        TestClass t1 = (TestClass) q.execute();
        t1.setByteArray(new byte[]{1,3});
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("_bArray"));
    }

    @Test
    public void testSimpleFailsFieldValueArraySize() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        Query q = pm.newQuery(TestClass.class, "_int == 12");
        q.setUnique(true);
        TestClass t1 = (TestClass) q.execute();
        t1.setByteArray(new byte[]{1,2,3});
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("_bArray"));
    }

    @Test
    public void testSimpleFailsObject1() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        TestClass t1 = new TestClass();
        pm.makePersistent(t1);
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("TestClass"));
    }

    @Test
    public void testSimpleFailsObject2() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM(DB2);
        pm.currentTransaction().begin();
        TestClass t1 = new TestClass();
        pm.makePersistent(t1);
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("TestClass"));
    }
}
