package org.zoodb.tools;

import java.io.StringWriter;

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
    }
    
}
