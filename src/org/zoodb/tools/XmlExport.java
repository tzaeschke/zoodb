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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;

/**
 * Export a database to xml.
 * 
 * @author ztilmann
 *
 */
public class XmlExport {

    private final Writer out;
    
    public XmlExport(Writer out) {
        this.out = out;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Error: invalid number of arguments.");
            System.out.println("Usage: ");
            System.out.println("    XmlExport <dbName> <xmlFileName>");
            return;
        }
        
        String dbName = args[0];
        String xmlName = args[1];
        Writer out = openFile(xmlName);
        if (out == null) {
            return;
        }
        
        try {
            new XmlExport(out).writeDB(dbName);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public void writeDB(String dbName) {
        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();
        
        writeln("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
        writeln("<database>");
        
        writeln("<schema>");
        for (ZooClass sch: ZooSchema.locateAllClasses(pm)) {
            if (sch.getJavaClass() == ZooPCImpl.class) {
                continue;
            }
            writeln("<class " +
                    "name=\"" + sch.getClassName() + 
//                    "\" oid=\"" + sch.getObjectId() + 
//                    "\" super=\"" + sch.getSuperClass().getClassName() + 
                    "\">");

            writeln("</class>");
        }
        writeln("</schema>");
        
        writeln("<data>");
        for (ZooClass sch: ZooSchema.locateAllClasses(pm)) {
            writeln("<class name=\"" + sch.getClassName() + "\">");
            Extent<?> ext = pm.getExtent(sch.getJavaClass());
            for (Object o: ext) {
                writeln("<object oid=\"" + (Long)JDOHelper.getObjectId(o) + "\">");
                
                writeln("</object>");
            }
            writeln("</class>");
        }
        writeln("</data>");
        
        writeln("</database>");
        
        
        pm.currentTransaction().rollback();
        pm.close();
        pmf.close();
        
    }

    private static Writer openFile(String xmlName) {
        File file = new File(xmlName);
        if (file.exists()) {
            System.out.println("File already exists: " + file);
            return null;
        }
        try {
            if (!file.createNewFile()) {
                System.out.println("Could not create file: " + file);
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not create file: " + file);
        }
        
        try {
            FileOutputStream fos = new FileOutputStream(file);
            return new OutputStreamWriter(fos, "UTF-8");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } 
    }
    
    private void writeln(String str) {
        try {
            out.append(str);
            out.append('\n');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
}
