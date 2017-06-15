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
package org.zoodb.tools.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBHashMap;
import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.SessionFactory;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.schema.ZooSchema;
import org.zoodb.tools.ZooConfig;

public class DataStoreManagerInMemory implements DataStoreManager {

	private static final String DEFAULT_FOLDER = "InMemory"; 

	private static final Map<String, ArrayList<ByteBuffer>> map = 
		new HashMap<String, ArrayList<ByteBuffer>>();
    
	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName The database file name or path 
	 */
	@Override
	public void createDb(String dbName) {
		String dbPath = getDbPath(dbName);
		if (map.containsKey(dbPath)) {
			throw DBLogger.newUser("Database already exists: " + dbPath);
		}
		map.put(dbPath, new ArrayList<ByteBuffer>());
		
		//create files

		//DB file
		FreeSpaceManager fsm = new FreeSpaceManager();
		StorageRootInMemory root = 
				new StorageRootInMemory(dbPath, "rw", ZooConfig.getFilePageSize(), fsm);
		StorageChannel file = root.createChannel();
		StorageChannelOutput out = file.getWriter(false);
		fsm.initBackingIndexNew(file);

		int headerPage = out.allocateAndSeek(PAGE_TYPE.DB_HEADER, 0);
		if (headerPage != 0) {
			throw DBLogger.newFatalInternal("Header page = " + headerPage);
		}
		int rootPage1 = out.allocateAndSeek(PAGE_TYPE.ROOT_PAGE, 0);
		int rootPage2 = out.allocateAndSeek(PAGE_TYPE.ROOT_PAGE, 0);

		//header: this is written further down

		//write User data
		int userData = out.allocateAndSeek(PAGE_TYPE.USERS, 0);

		//dir for schemata
		int schemaData = out.allocateAndSeekAP(PAGE_TYPE.SCHEMA_INDEX, 0, -1);
		//ID of next page
		out.writeInt(0);
		//Schema ID / schema data (page or actual data?)
		//0 for no more schemata
		out.writeInt(0);


		//dir for indices
		int indexDirPage = out.allocateAndSeek(PAGE_TYPE.INDEX_CATALOG, 0);
		//ID of next page
		out.writeInt(0);
		//Schema ID / attribute ID / index type / Page ID
		//0 for nor more indices
		out.writeInt(0);

		//OID index
		PagedOidIndex oidIndex = new PagedOidIndex(file);
		int oidPage = oidIndex.write();

		//Free space index
		int freeSpacePg = fsm.write();
		
		//write header
		out.seekPageForWrite(PAGE_TYPE.DB_HEADER, headerPage);
		out.writeInt(DiskIO.DB_FILE_TYPE_ID);
		out.writeInt(DiskIO.DB_FILE_VERSION_MAJ);
		out.writeInt(DiskIO.DB_FILE_VERSION_MIN);
		out.writeInt(ZooConfig.getFilePageSize());
		out.writeInt(rootPage1);
		out.writeInt(rootPage2);

		writeRoot(out, rootPage1, 1, userData, oidPage, schemaData, indexDirPage, freeSpacePg, 
				fsm.getPageCount());
		writeRoot(out, rootPage2, 0, userData, oidPage, schemaData, indexDirPage, freeSpacePg, 
				fsm.getPageCount());

		file.close();
		file = null;
		root.close();
		root = null;
		out = null;


		//initial schemata
		Properties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		pm.currentTransaction().begin();

		ZooSchema schema = ZooJdoHelper.schema(pm);
		schema.addClass(PersistenceCapableImpl.class);
		schema.addClass(DBHashMap.class);
		schema.addClass(DBArrayList.class);

		pm.currentTransaction().commit();
		pm.close();
		pmf.close();
	}

	private void writeRoot(StorageChannelOutput raf, int pageID, int txID, int userPage, 
			int oidPage, int schemaPage, int indexPage, int freeSpaceIndexPage, int pageCount) {
		raf.seekPageForWrite(PAGE_TYPE.ROOT_PAGE, pageID);
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
	public boolean removeDb(String dbName) {
		String dbPath = getDbPath(dbName);
//		if (map.remove(dbPath) == null) { 
//			throw DBLogger.newUser("DB does not exist: " + dbPath);
//		}

		//TODO whoo! This is sooo dirty! 
		Path path = FileSystems.getDefault().getPath(dbPath); 
		SessionFactory.cleanUp(path.toFile());
		return map.remove(dbPath) != null;
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
