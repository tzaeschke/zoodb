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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * Export a database to xml.
 * 
 * @author ztilmann
 *
 */
public class XmlImport {

    //private static InputStreamReader in;
    private static Scanner scanner;
    
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Error: invalid number of arguments.");
            System.out.println("Usage: ");
            System.out.println("    XmlExport <dbName> <xmlFileName>");
            return;
        }
        
        String dbName = args[0];
        String xmlName = args[1];
        openFile(xmlName);
        
        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();
        
        readln("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
        readln("<database>");
        
        readln("<schema>");
        for (ZooClass sch: ZooSchema.getAllClasses(pm)) {
            if (sch.getJavaClass() == PersistenceCapableImpl.class) {
                continue;
            }
            readln("<class");
            scanner.skip("name=\"");
            String name = read(); 
//                    "\" oid=\"" + sch.getObjectId() + 
//                    "\" super=\"" + sch.getSuperClass().getClassName() +
            try {
                Class<?> cls = Class.forName(name);
                ZooSchema.defineClass(pm, cls);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            
            readln("\">");
            readln("</class>");
        }
        readln("</schema>");
        
        readln("<data>");
        for (ZooClass sch: ZooSchema.getAllClasses(pm)) {
            readln("<class");
            scanner.skip("name=\"");
            String name = read();
            readln("\">");
            
            Extent<?> ext = pm.getExtent(sch.getJavaClass());
            for (Object o: ext) {
                readln("<object");
                scanner.skip("oid=\"");
                long oid = Long.parseLong(read());
                readln("\">");
                
                readln("</object>");
            }
            readln("</class>");
        }
        readln("</data>");
        
        readln("</database>");
        
        
        pm.currentTransaction().commit();
        pm.close();
        pmf.close();
        
//        try {
//            in.close();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        scanner.close();
    }

    private static void openFile(String xmlName) {
        File file = new File(xmlName);
        if (!file.exists()) {
            System.out.println("File not found: " + file);
            return;
        }
        
        try {
            FileInputStream fis = new FileInputStream(file);
            //InputStreamReader in = new InputStreamReader(fos, "UTF-8");
            scanner = new Scanner(fis, "UTF-8");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
//        } catch (UnsupportedEncodingException e) {
//            throw new RuntimeException(e);
        } 
    }
    
    private static String read() {
        return scanner.next();
//        try {
//            in.append(str);
//            in.append('\n');
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }
    
    private static void readln(String str) {
        String s2 = scanner.next();
        if (!s2.equals(str)) {
            throw new IllegalStateException("Expected: " + str + " but got: " + s2);
        }
//        try {
//            in.append(str);
//            in.append('\n');
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }
    
}
