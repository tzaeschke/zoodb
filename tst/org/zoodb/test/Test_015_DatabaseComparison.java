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
package org.zoodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Scanner;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooXmlExport;
import org.zoodb.tools.ZooXmlImport;
import org.zoodb.tools.ZooCompareDb;

public class Test_015_DatabaseComparison {

	private static final String DB2 = "TestDb2";
	
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
    }
    
    private void copyDB() {
    	//rename classes to avoid conflicts with other tests through DataDeSerializer's static
    	//storage of Constructors and Fields
    	PersistenceManager pm = TestTools.openPM();
    	pm.currentTransaction().begin();
    	renameClass(pm, TestClass.class.getName(), "TestCls");
    	renameClass(pm, TestSerializer.class.getName(), "TestSer");
    	pm.currentTransaction().commit();
    	TestTools.closePM();
    	
        StringWriter out = new StringWriter();
        ZooXmlExport ex = new ZooXmlExport(out);
        ex.writeDB(TestTools.getDbName());
        
        //System.out.println(out.getBuffer());
        
        Scanner sc = new Scanner(new StringReader(out.getBuffer().toString())); 
        ZooXmlImport im = new ZooXmlImport(sc);
        im.readDB(DB2);

        //revert renaming
    	pm = TestTools.openPM();
    	pm.currentTransaction().begin();
    	renameClass(pm, "TestCls", TestClass.class.getName());
    	renameClass(pm, "TestSer", TestSerializer.class.getName());
    	pm.currentTransaction().commit();
    	TestTools.closePM();
    }
    
    private void renameClass(PersistenceManager pm, String oldName, String newName) {
    	if (ZooSchema.locateClass(pm, oldName) != null) {
    		ZooSchema.locateClass(pm, oldName).rename(newName);
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

    private void populateComplex(PersistenceManager pm) {
        pm.currentTransaction().begin();
        TestSerializer ts1 = new TestSerializer();
        ts1.init();
        pm.makePersistent(ts1);
        pm.currentTransaction().commit();
    }
    
    @Test
    public void testComplexClass() {
    	//populate
       	TestTools.defineSchema(TestSerializer.class, TestSuper.class);
        PersistenceManager pm = TestTools.openPM();
        populateComplex(pm);
        TestTools.closePM();
        pm = null;

       	TestTools.defineSchema(DB2, TestSerializer.class, TestSuper.class);
        PersistenceManager pm2 = TestTools.openPM(DB2);
        populateComplex(pm2);
        TestTools.closePM(pm2);
        pm2 = null;
    
        copyDB();
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertEquals("", result);
    }
    
    @Test
    public void testSimpleFailsClasses1() {
    	populateSimple();
        copyDB();

        TestTools.defineSchema(TestSerializer.class);
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains(TestSerializer.class.getName()));
    }

    @Test
    public void testSimpleFailsClasses2() {
    	populateSimple();
        copyDB();

        TestTools.defineSchema(DB2, TestSerializer.class);
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains(TestSerializer.class.getName()));
    }

    @Test
    public void testSimpleFailsFieldName1() {
    	populateSimple();
        copyDB();

        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        ZooSchema.locateClass(pm, TestClass.class).declareField("dummy", Integer.TYPE);
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
        ZooSchema.locateClass(pm, TestClass.class).declareField("dummy", Integer.TYPE);
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
        ZooSchema.locateClass(pm, TestClass.class).declareField("dummy", Integer.TYPE);
        pm.currentTransaction().commit();
        TestTools.closePM();
        
        PersistenceManager pm2 = TestTools.openPM(DB2);
        pm2.currentTransaction().begin();
        ZooSchema.locateClass(pm2, TestClass.class).declareField("dummy", Integer.class);
        pm2.currentTransaction().commit();
        TestTools.closePM();
        
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertTrue(result, result.contains("dummy"));
    }


    //TODO check that differences are found
}
