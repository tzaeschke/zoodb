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

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Scanner;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.DataStoreManager;
import org.zoodb.test.api.TestSerializer;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooHelper;
import org.zoodb.tools.ZooXmlExport;
import org.zoodb.tools.ZooXmlImport;
import org.zoodb.tools.internal.XmlReader;
import org.zoodb.tools.internal.XmlWriter;

public class Test_014_XmlImportExport {

	private static final String DB2 = "TestDb2";
	private static final String FILE = "TestDb.xml";
	
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
        
        ZooXmlExport ex = new ZooXmlExport(out);
        ex.writeDB(TestTools.getDbName());
        
        //System.out.println(out.getBuffer());
        
        Scanner sc = new Scanner(new StringReader(out.getBuffer().toString())); 
        ZooXmlImport im = new ZooXmlImport(sc);
        im.readDB(TestTools.getDbName());
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
	public void testReaderWriter() {
		byte b1 = -1;
		byte b2 = Byte.MIN_VALUE;
		byte b3 = Byte.MAX_VALUE;
		int i1 = -1;
		int i2 = Integer.MAX_VALUE;
		int i3 = Integer.MIN_VALUE;
		long l1 = -1L;
		long l2 = Long.MAX_VALUE;
		long l3 = Long.MIN_VALUE;
		char c1 = 1;
		char c2 = 255;
		char c3 = 64000;
		char c4 = '\n';
		char c5 = '\0';
		String s1 = "\n xxxx \\ \\\\ Hallo";
		byte[] ba1 = new byte[]{1,2,3};
		
		Writer w = new StringWriter();
		XmlWriter xw = new XmlWriter(w);
		xw.startField(0);
		xw.writeBoolean(true);
		xw.writeBoolean(false);
		xw.writeByte(b1);
		xw.writeByte(b2);
		xw.writeByte(b3);
		xw.writeInt(i1);
		xw.writeInt(i2);
		xw.writeInt(i3);
		xw.writeLong(l1);
		xw.writeLong(l2);
		xw.writeLong(l3);
		xw.writeChar(c1);
		xw.writeChar(c2);
		xw.writeChar(c3);
		xw.writeChar(c4);
		xw.writeChar(c5);
		xw.writeString(s1);
		xw.write(ba1);
		xw.finishField();
		
		Scanner scanner = new Scanner(w.toString());
		XmlReader xr = new XmlReader(scanner);
		xr.startReadingField(0);
		assertEquals(true, xr.readBoolean());
		assertEquals(false, xr.readBoolean());
		assertEquals(b1, xr.readByte());
		assertEquals(b2, xr.readByte());
		assertEquals(b3, xr.readByte());
		assertEquals(i1, xr.readInt());
		assertEquals(i2, xr.readInt());
		assertEquals(i3, xr.readInt());
		assertEquals(l1, xr.readLong());
		assertEquals(l2, xr.readLong());
		assertEquals(l3, xr.readLong());
		assertEquals(c1, xr.readChar());
		assertEquals(c2, xr.readChar());
		assertEquals(c3, xr.readChar());
		assertEquals(c4, xr.readChar());
		assertEquals(c5, xr.readChar());
		assertEquals(s1, xr.readString());		
		byte[] ba1_ = new byte[ba1.length];
		xr.readFully(ba1_);
		assertTrue(Arrays.equals(ba1, ba1_));
		xr.stopReadingField();
	}
    
    @Test
    public void testSimpleClasses() {
    	//populate
    	populateSimple();

    	StringWriter out = new StringWriter();

    	ZooXmlExport ex = new ZooXmlExport(out);
    	ex.writeDB(TestTools.getDbName());

    	//System.out.println(out.getBuffer());

    	Scanner sc = new Scanner(new StringReader(out.getBuffer().toString())); 
    	ZooXmlImport im = new ZooXmlImport(sc);
    	im.readDB(TestTools.getDbName());

    }

    @Test
    public void testSimpleClassesToFile() {
    	//populate
    	populateSimple();
    	String file = System.getProperty("user.home") + File.separator + FILE;
    	File f = new File(file);
    	if (f.exists()) {
    		f.delete();
    	}
    	
    	ZooXmlExport.main(new String[]{TestTools.getDbName(), file});
    	
    	ZooXmlImport.main(new String[]{TestTools.getDbName(), file});
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
    	ZooXmlExport ex = new ZooXmlExport(out);
    	ex.writeDB(TestTools.getDbName());
    	System.out.println(out.getBuffer());
    	Scanner sc = new Scanner(new StringReader(out.getBuffer().toString())); 
    	ZooXmlImport im = new ZooXmlImport(sc);
    	
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
    }
    
    /**
     * Test import of ZooDB 0.3 xml files.
     */
    @Test
    public void testImport0_3() {
		String path = Test_014_XmlImportExport.class.getResource("XmlComplexTest.xml").getPath();
		
       	//import to DB
    	TestTools.defineSchema(TestSerializer.class, TestSuper.class);
    	ZooXmlImport.main(new String[]{TestTools.getDbName(), path});
        
    	
        
        //check target
        PersistenceManager pm2 = TestTools.openPM(DB2);
        pm2.currentTransaction().begin();
                
        //Check for content in target
        Extent<TestSerializer> ext = pm2.getExtent(TestSerializer.class); 
        TestSerializer ts2 = ext.iterator().next();
        ts2.check(false);
        pm2.currentTransaction().rollback();
        TestTools.closePM();
    }
}
