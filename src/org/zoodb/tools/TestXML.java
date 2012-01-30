package org.zoodb.tools;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Scanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.test.util.TestTools;

public class TestXML {

    @Before
    public void before() {
        TestTools.createDb();
    }
    
    @After
    public void after() {
        TestTools.closePM();
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
    
}
