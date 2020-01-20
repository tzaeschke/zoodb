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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.test.testutil.TestTools;
import org.zoodb.tools.ZooCheckDb;
import org.zoodb.tools.ZooCompareDb;

public class Test_017_DatabaseVersioning {

	private static final String DB2 = TestTools.getDbName() + "2";
    private static final String DB_0_5_2 = "TestDb_0.5.2.zdb";
    private static final String DB_0_0_0 = "TestDb_0.0.0.zdb";
    private static final String DB_999_999_999 = "TestDb_999.999.999.zdb";
    private static final String DB_0_5_2_wrecked = "TestDb_0.5.2.wrecked.zdb";
	
	
    @Before
    public void before() {
        TestTools.createDb();
        ZooCompareDb.logToConsole = false;
    }
    
    @After
    public void after() {
        ZooCompareDb.logToConsole = true;
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
    
    private String copyDB(String orig) {
    	String origPath = 
    			Test_017_DatabaseVersioning.class.getResource(orig).getPath().substring(1);
        System.err.println("xorig    =" + orig);
        System.err.println("xorigPath=" + origPath);
        try {
            Path p22 = Paths.get(origPath); 
            try {
                Files.find(p22, 10, (p3, a) -> p3.toString().contains("wrecked"))
                    .forEach(p -> System.out.println("Found0: " + p));
            } catch (IOException e) {
                System.err.println("Error: " + e.getMessage());
            }
            System.err.println("orig22  =" + p22);
            Path p03 = p22.subpath(0, 3);
            System.err.println("orig03  =" + p03);
            Path p04 = p22.getParent().getParent().getParent().getParent();
            System.err.println("orig04  =" + p04);
            try {
                Files.find(p04, 10, (p3, a) -> p3.toString().contains("wrecked"))
                .forEach(p -> System.err.println("Found4: " + p));
            } catch (IOException e) {
                System.err.println("Error4: " + e.getMessage());
            }

	        Path p2 = FileSystems.getDefault().getPath(TestTools.getDbFileName() + "2");
	        removeFile(p2);
	        Path pOrig = Paths.get(origPath); 
	        Files.copy(pOrig, p2);
        } catch (IOException e) {
        	throw new RuntimeException(e);
        }
        return origPath;
    }
    
	private void populateSimple() {
		TestTools.defineSchema(TestClass.class);
		TestTools.defineIndex(TestClass.class, "_int", true);
		TestTools.defineIndex(TestClass.class, "_long", false);
		TestTools.defineIndex(TestClass.class, "_string", false);
		TestTools.defineIndex(TestClass.class, "_double", true);
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
        TestTools.closePM();
 	}

	
    @SuppressWarnings("unchecked")
	@Test
    public void testSimpleClasses_0_5_2() {
    	// populate DB
    	populateSimple();

    	// Copy DB_0_5_2 to DB2
    	String db2Path = copyDB(DB_0_5_2);

        // Check DB2 file version
        ZooCheckDb.enableStringOutput();
        ZooCheckDb.main(db2Path);
        String output = ZooCheckDb.getStringOutput();
        assertTrue(output.contains("format version: 1.5"));        
        
        // Compare TODO this should fail in future unless we evolve the DB
        String result = ZooCompareDb.run(TestTools.getDbName(), DB2);
        assertEquals("", result);
        
        // Run some tests
        PersistenceManager pm = TestTools.openPM(DB2);
        pm.currentTransaction().begin();
        // Test index query
        Query q = pm.newQuery(TestClass.class, "_int == 12 || _int == 123");
        List<TestClass> c = (List<TestClass>) q.execute();
        assertTrue(c.get(0).getInt() == 12);
        assertTrue(c.get(1).getInt() == 123);
        assertEquals(2,  c.size());
        q.close(c);
        
        pm.currentTransaction().commit();
        pm.close();
    }
    
    
    @Test
    public void testFailure_0_0_0() {
        // Copy DB_0_5_2 to DB2
        String db2Path = copyDB(DB_0_0_0);

        // Check DB2 file version
        ZooCheckDb.enableStringOutput();
        ZooCheckDb.main(db2Path);
        String output = ZooCheckDb.getStringOutput();
        assertTrue(output.contains("format version: 0.0"));        
        assertTrue(output.contains("Illegal major file version: "));

        try {
            TestTools.openPM(DB2);
            fail();
        } catch (RuntimeException e) {
            DBLogger.isFatalDataStoreException(e);
            assertTrue(e.getMessage().contains("Illegal major file version: "));
        }
    }

    @Test
    public void testFailure_999_999_999() {
        // Copy DB_0_5_2 to DB2
        String db2Path = copyDB(DB_999_999_999);

        // Check DB2 file version
        ZooCheckDb.enableStringOutput();
        ZooCheckDb.main(db2Path);
        String output = ZooCheckDb.getStringOutput();
        assertTrue(output.contains("format version: 999.999"));        
        assertTrue(output.contains("Illegal major file version: "));

        try {
            TestTools.openPM(DB2);
            fail();
        } catch (RuntimeException e) {
            DBLogger.isFatalDataStoreException(e);
            assertTrue(e.getMessage().contains("Illegal major file version: "));
        }
    }

    @Test
    public void testFailure_0_5_2_wrecked() {
        // Copy DB_0_5_2_wrecked to DB2
        String db2Path = copyDB(DB_0_5_2_wrecked);

        // Check DB2 file version
        ZooCheckDb.enableStringOutput();
        ZooCheckDb.main(db2Path);
        String output = ZooCheckDb.getStringOutput();
        assertTrue(output.contains("This is not a ZooDB file (illegal file ID:"));

        // try to open DB
        try {
            TestTools.openPM(DB2);
            fail();
        } catch (RuntimeException e) {
            DBLogger.isFatalDataStoreException(e);
            assertTrue(e.getMessage().contains("This is not a ZooDB file (illegal file ID:"));
        }
        TestTools.closePM();
    }

}
