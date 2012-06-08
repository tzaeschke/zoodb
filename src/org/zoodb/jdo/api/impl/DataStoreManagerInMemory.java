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
package org.zoodb.jdo.api.impl;

import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_TYPE_ID;
import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_VERSION_MAJ;
import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_VERSION_MIN;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.DBArrayList;
import org.zoodb.jdo.api.DBHashMap;
import org.zoodb.jdo.api.DataStoreManager;
import org.zoodb.jdo.api.ZooConfig;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.PageAccessFileInMemory;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class DataStoreManagerInMemory implements DataStoreManager {

	private static final String DEFAULT_FOLDER = "InMemory"; 

	private static final Map<String, ArrayList<ByteBuffer>> map = 
		new HashMap<String, ArrayList<ByteBuffer>>();
    
	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	@Override
	public void createDb(String dbName) {
		String dbPath = getDbPath(dbName);
		if (map.containsKey(dbPath)) {
			throw new JDOUserException("Database already exists: " + dbPath);
		}
		map.put(dbPath, new ArrayList<ByteBuffer>());
		
		//create files
		PageAccessFileInMemory raf = null;

		//DB file
		FreeSpaceManager fsm = new FreeSpaceManager();
		raf = new PageAccessFileInMemory(dbPath, "rw", ZooConfig.getFilePageSize(), fsm);
		fsm.initBackingIndexNew(raf);

		int headerPage = raf.allocateAndSeek(0);
		if (headerPage != 0) {
			throw new JDOFatalDataStoreException("Header page = " + headerPage);
		}
		int rootPage1 = raf.allocateAndSeek(0);
		int rootPage2 = raf.allocateAndSeek(0);

		//header: this is written further down

		//write User data
		int userData = raf.allocateAndSeek(0);

		//dir for schemata
		int schemaData = raf.allocateAndSeek(0, -1);
		//ID of next page
		raf.writeInt(0);
		//Schema ID / schema data (page or actual data?)
		//0 for no more schemata
		raf.writeInt(0);


		//dir for indices
		int indexDirPage = raf.allocateAndSeek(0);
		//ID of next page
		raf.writeInt(0);
		//Schema ID / attribute ID / index type / Page ID
		//0 for nor more indices
		raf.writeInt(0);

		//OID index
		PagedOidIndex oidIndex = new PagedOidIndex(raf);
		int oidPage = oidIndex.write();

		//Free space index
		int freeSpacePg = fsm.write();
		
		//write header
		raf.seekPageForWrite(headerPage);
		raf.writeInt(DB_FILE_TYPE_ID);
		raf.writeInt(DB_FILE_VERSION_MAJ);
		raf.writeInt(DB_FILE_VERSION_MIN);
		raf.writeInt(ZooConfig.getFilePageSize());
		raf.writeInt(rootPage1);
		raf.writeInt(rootPage2);

		writeRoot(raf, rootPage1, 1, userData, oidPage, schemaData, indexDirPage, freeSpacePg, 
				fsm.getPageCount());
		writeRoot(raf, rootPage2, 0, userData, oidPage, schemaData, indexDirPage, freeSpacePg, 
				fsm.getPageCount());

		raf.close();
		raf = null;


		//initial schemata
		Properties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();

		ZooSchema.defineClass(pm, PersistenceCapableImpl.class);
		ZooSchema.defineClass(pm, DBHashMap.class);
		ZooSchema.defineClass(pm, DBArrayList.class);

		pm.currentTransaction().commit();
		pm.close();
		pmf.close();
	}

	private void writeRoot(PageAccessFile raf, int pageID, int txID, int userPage, int oidPage, 
			int schemaPage, int indexPage, int freeSpaceIndexPage, int pageCount) {
		raf.seekPageForWrite(pageID);
		//txID
		raf.writeLong(txID);
		//User table
		raf.writeInt(userPage);
		//OID table
		raf.writeInt(oidPage);
		//schemata
		raf.writeInt(schemaPage);
		//indices
		raf.writeInt(indexPage);
		//free space index
		raf.writeInt(freeSpaceIndexPage);
		//page count
		raf.writeInt(pageCount);
		//last used oid
		raf.writeLong(100);
		//txID
		raf.writeLong(txID);
	}
	
	@Override
	public void removeDb(String dbName) {
		String dbPath = getDbPath(dbName);
		if (map.remove(dbPath) == null) { 
			throw new JDOUserException("DB does not exist: " + dbPath);
		}
	}

	@Override
	public String getDefaultDbFolder() {
		return DEFAULT_FOLDER;
	}

	@Override
	public String getDbPath(String dbName) {
        if (dbName.contains("\\") || dbName.contains("/") || dbName.contains(File.separator)) {
            return dbName;
        }
        return DEFAULT_FOLDER + File.separator + dbName;
	}

	@Override
	public boolean dbExists(String dbName) {
		return map.containsKey(getDbPath(dbName));	}

	public static ArrayList<ByteBuffer> getInternalData(String dbPath) {
		if (!map.containsKey(dbPath)) {
			throw new IllegalStateException("DB not found: " + dbPath);
		}
		return map.get(dbPath);
	}
}
