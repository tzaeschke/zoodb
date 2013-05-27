/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Scanner;

import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.DataStoreManager;
import org.zoodb.jdo.api.ZooHelper;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.XmlExport;
import org.zoodb.tools.XmlImport;

public class Test_014_XmlImportExport {

	private static final String DB2 = "TestDb2";
	
    @Before
    public void before() {
        TestTools.createDb();
    }
    
    @After
    public void after() {
        TestTools.closePM();
        TestTools.removeDb();
    }
    
    @Test
    public void testEmptyDB() {
        StringWriter out = new StringWriter();
        
        XmlExport ex = new XmlExport(out);
        ex.writeDB(TestTools.getDbName());
        
        System.out.println(out.getBuffer());
        
        Scanner sc = new Scanner(new StringReader(out.getBuffer().toString())); 
        XmlImport im = new XmlImport(sc);
        im.readDB(TestTools.getDbName());
        
        //heyho! Reading empty DB did not crash :-)
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

    	StringWriter out = new StringWriter();

    	XmlExport ex = new XmlExport(out);
    	ex.writeDB(TestTools.getDbName());

    	System.out.println(out.getBuffer());

    	Scanner sc = new Scanner(new StringReader(out.getBuffer().toString())); 
    	XmlImport im = new XmlImport(sc);
    	im.readDB(TestTools.getDbName());

    }

    @Test
    public void testComplexClass() {
    	//populate
        Object oid = null;
       	TestTools.defineSchema(TestSerializer.class, TestSuper.class);
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        TestSerializer ts1 = new TestSerializer();
        ts1.init();
        ts1.check(true);
        pm.makePersistent(ts1);
        oid = pm.getObjectId(ts1);
        pm.currentTransaction().commit();
        TestTools.closePM();
        pm = null;

        TestSerializer.resetStatic();
        
        
        //export to XML
    	StringWriter out = new StringWriter();
    	XmlExport ex = new XmlExport(out);
    	ex.writeDB(TestTools.getDbName());
    	System.out.println(out.getBuffer());
    	Scanner sc = new Scanner(new StringReader(out.getBuffer().toString())); 
    	XmlImport im = new XmlImport(sc);
    	
    	//import to new DB
    	DataStoreManager dsm = ZooHelper.getDataStoreManager();
    	if (dsm.dbExists(DB2)) {
    		dsm.removeDb(DB2);
    	}
    	dsm.createDb(DB2);
    	TestTools.defineSchema(DB2, TestSerializer.class, TestSuper.class);
    	im.readDB(DB2);
        
    	
        
        //check target
        PersistenceManager pm2 = TestTools.openPM(DB2);
        pm2.currentTransaction().begin();
                
        //Check for content in target
        TestSerializer ts2 = (TestSerializer) pm2.getObjectById(oid, true);
        ts2.check(false);
        pm2.currentTransaction().rollback();
        TestTools.closePM();

        TestSerializer.resetStatic();

        //Now try the same thing again, this time with an existing object.
        pm2 = TestTools.openPM(DB2);
        pm2.currentTransaction().begin();

        TestSerializer ts3 = (TestSerializer) pm2.getObjectById(oid);
        ts3.check(false);
        //mark dirty to enforce rewrite.
        ts3.markDirtyTS();
        pm2.currentTransaction().commit();
        TestTools.closePM();
            
            
        TestSerializer.resetStatic();
        //Check target
        pm2 = TestTools.openPM(DB2);
        pm2.currentTransaction().begin();

        TestSerializer ts4 = (TestSerializer) pm2.getObjectById(oid, true);
        ts4.check(false);
        pm2.currentTransaction().rollback();
        TestTools.closePM();

        
        
        
//    	StringWriter out = new StringWriter();
//    	XmlExport ex = new XmlExport(out);
//    	ex.writeDB(TestTools.getDbName());
//    	System.out.println(out.getBuffer());
//    	Scanner sc = new Scanner(new StringReader(out.getBuffer().toString())); 
//    	XmlImport im = new XmlImport(sc);
//    	im.readDB(TestTools.getDbName());

    }

    //TODO test export of Strings, e.g. containing '\' or " or ' or CR/LF.
    
    //TODO idea for test:
    //- populate DB
    //- export
    //- import into other
    //- export again to new file
    //- exported XML files should be identical, possibly exception OIDs
    //--> Verifies that import works, but export may still be wrong...
    
    //TODO test with purely artificial classes/instances
}
