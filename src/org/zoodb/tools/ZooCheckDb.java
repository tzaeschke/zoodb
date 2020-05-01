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

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.internal.Session;
import org.zoodb.internal.server.FileHeader;
import org.zoodb.internal.server.SessionFactory;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.tools.internal.ZooCommandLineTool;

public class ZooCheckDb extends ZooCommandLineTool {

	private static final String DB_NAME = "TestDb"; 
	//private static final String DB_NAME = "RandomRegularGraph-n1000-d20";
	//private static final String DB_NAME = "zoodb"; 
	//private static final String DB_NAME = "D:\\data\\SEDD-12-08\\stackoverflow2.zdb";
	//private static final String DB_NAME = "StackBicycles";
	//private static final String DB_NAME = "StackServerFault";

	public static void main(String ... args) {
		String dbName;
		if (args.length == 0) {
			dbName = DB_NAME;
		} else {
			dbName = args[0];
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
		
		
		out.println("Checking database: " + dbName);

		ZooJdoProperties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		Session s = (Session) pm.getDataStoreConnection().getNativeConnection();
		String report = s.getPrimaryNode().checkDb();
		out.println();
		out.println("Report");
		out.println("======");

		out.println(report);

		out.println("======");
		pm.close();
		pmf.close();
		out.println("Checking database done.");
	}

}
