/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.tools;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.internal.Session;
import org.zoodb.internal.server.FileHeader;
import org.zoodb.internal.server.SessionFactory;
import org.zoodb.internal.util.FormattedStringBuilder;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooField;
import org.zoodb.tools.internal.ZooCommandLineTool;

public class ZooCheckDb extends ZooCommandLineTool {

    private static final String DB_NAME = "TestDb.zdb";

    private static final String RULE = "================================";

    private static boolean longNames = false;

    public static void main(String... args) {
        String dbName;
        boolean listSchema = true;
        boolean listIndexes = true;

        if (Arrays.binarySearch(args, "--help") >= 0 || Arrays.binarySearch(args, "-help") >= 0) {
            out.println("Usage: ZooCheckDb <options> databaseFileName");
            out.println("    This tools performs a basic sanity check of the database and prints ");
            out.println("    out database statistics. Print out of additional information can be ");
            out.println("    controlled by <options>.");
            out.println("Options:");
            out.println("    --help:           Print help");
            out.println("    --list-indexes:   List all database indexes");
            out.println("    --list-schema:    List all schemata");
            out.println("    --long-names:     Print fully qualified class and type names");
            return;
        }

        if (args.length == 0) {
            dbName = DB_NAME;
        } else {
            dbName = args[args.length - 1];
            listSchema = Arrays.binarySearch(args, "--list-schema") >= 0;
            listIndexes = Arrays.binarySearch(args, "--list-indexes") >= 0;
            longNames = Arrays.binarySearch(args, "--long-names") >= 0;
        }

        if (!ZooHelper.getDataStoreManager().dbExists(dbName)) {
            err.println("ERROR Database not found: " + dbName);
            return;
        }

        //read header
        out.println("Database file info: " + dbName);
        Path path = Paths.get(ZooHelper.getDataStoreManager().getDbPath(dbName));
        FileHeader header = SessionFactory.readHeader(path);
        if (!header.successfulRead()) {
            out.println("ERROR reading file: " + header.errorMsg());
        }
        out.println("magic number: " + Integer.toHexString(header.getFileID()));
        out.println("format version: " + header.getVersionMajor() + "." + header.getVersionMinor());
        out.println("page size: " + header.getPageSize());
        out.print("root page IDs: ");
        for (int id : header.getRootPages()) {
            out.print(id + ", ");
        }
        out.println();
        out.println();

        if (!header.successfulRead()) {
            out.println("Aborting due to error.");
            return;
        }


        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        Session s = (Session) pm.getDataStoreConnection().getNativeConnection();
        String report = s.getPrimaryNode().checkDb();
        out.println();
        out.println("Check / Statistics");
        out.println(RULE);
        out.print(report);
        out.println(RULE);
        pm.close();
        pmf.close();
        out.println();

        if (listSchema) {
            listSchema(dbName);
        }

        if (listIndexes) {
            listIndexes(dbName);
        }

        out.println("Checking database done.");
    }

    private static void listIndexes(String dbName) {
        out.println("Index Report");
        out.println(RULE);

        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();
        FormattedStringBuilder s = new FormattedStringBuilder();
        int nIndexes = 0;
        for (ZooClass cls : ZooJdoHelper.schema(pm).getAllClasses()) {
            for (ZooField f : cls.getAllFields()) {
                if (f.hasIndex()) {
                    nIndexes++;
                    if (f.isIndexUnique()) {
                        s.append(" unique index:     ");
                    } else {
                        s.append(" non-unique index: ");
                    }
                    s.append(className(cls.getName()));
                    s.appendln(".", f.getName());
                    s.appendln(" ", className(f.getTypeName()));
                }
            }
        }
        s.appendln("Indexes found: " + nIndexes);
        s.appendln(RULE);
        s.appendln();
        out.print(s);
        pm.currentTransaction().rollback();
        pm.close();
        pmf.close();
    }

    private static void listSchema(String dbName) {
        out.println("Schema Report");
        out.println(RULE);

        ZooJdoProperties props = new ZooJdoProperties(dbName);
        PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
        PersistenceManager pm = pmf.getPersistenceManager();
        pm.currentTransaction().begin();
        FormattedStringBuilder s = new FormattedStringBuilder();
        int nIndexes = 0;
        int nClasses = 0;
        int nFields = 0;
        for (ZooClass cls : ZooJdoHelper.schema(pm).getAllClasses()) {
            nClasses++;
            s.append(className(cls.getName()));
            if (cls.getSuperClass() != null &&
                    !cls.getSuperClass().getName().startsWith("org.zoodb")) {
                s.append(" : ");
                s.append(className(cls.getSuperClass().getName()));
            }
            s.appendln(" {");
            for (ZooField f : cls.getAllFields()) {
                nFields++;
                s.append("  " + className(f.getTypeName()) + " " + f.getName());
                if (f.hasIndex()) {
                    nIndexes++;
                    s.append(" indexed");
                    if (f.isIndexUnique()) {
                        s.append(" unique");
                    }
                }
                s.appendln();
            }
            s.appendln("}");
        }
        s.appendln("Classes found: " + nClasses);
        s.appendln("Fields found:  " + nFields);
        s.appendln("Indexes found: " + nIndexes);
        s.appendln(RULE);
        s.appendln();
        out.print(s);
        pm.currentTransaction().rollback();
        pm.close();
        pmf.close();
    }

    private static String className(String name) {
        if (longNames) {
            return name;
        }
        int pos = name.lastIndexOf('.');
        if (pos >= 0) {
            return name.substring(pos + 1);
        }
        return name;
    }

}
