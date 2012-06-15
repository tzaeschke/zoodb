/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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
    
}
