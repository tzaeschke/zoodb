/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_TYPE_ID;
import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_VERSION_MAJ;
import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_VERSION_MIN;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import javax.jdo.JDOException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBHashMap;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.internal.server.StorageChannel;
import org.zoodb.jdo.internal.server.StorageChannelOutput;
import org.zoodb.jdo.internal.server.StorageRootFile;
import org.zoodb.jdo.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.util.DBLogger;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.schema.ZooSchema;
import org.zoodb.tools.ZooConfig;

public class DataStoreManagerOneFile implements DataStoreManager {

	private static final String DEFAULT_FOLDER = 
		System.getProperty("user.home") + File.separator + "zoodb"; 
	
	private String toPath(String dbName) {
	    if (dbName.contains("\\") || dbName.contains("/") || dbName.contains(File.separator)) {
	        return dbName;
	    }
	    return DEFAULT_FOLDER + File.separator + dbName;
	}
	
	
	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	@Override
	public void createDb(String dbName) {
	    String dbPath = toPath(dbName);
        DBLogger.debugPrint(1, "Creating DB file: " + dbPath);
        String folderPath = dbPath.substring(0, dbPath.lastIndexOf(File.separator));
        File dbDir = new File(folderPath);
        if (!dbDir.exists()) {
            createDbFolder(dbDir);
            DBLogger.debugPrint(1, "Creating DB folder: " + dbDir.getAbsolutePath());
        }

		
		//create files
		StorageChannel file = null;
		try {
			//DB file
			File dbFile = new File(toPath(dbName));
			if (dbFile.exists()) {
				throw DBLogger.newUser("ZOO: DB already exists: " + dbFile);
			}
			if (!dbFile.createNewFile()) {
				throw DBLogger.newUser("ZOO: Error creating DB file: " + dbFile);
			}
			FreeSpaceManager fsm = new FreeSpaceManager();
			file = new StorageRootFile(dbPath, "rw",
					ZooConfig.getFilePageSize(), fsm);
			StorageChannelOutput out = file.getWriter(false);
			fsm.initBackingIndexNew(file);
			
			int headerPage = out.allocateAndSeek(DATA_TYPE.DB_HEADER, 0);
			if (headerPage != 0) {
				throw DBLogger.newFatal("Header page = " + headerPage);
			}
			int rootPage1 = out.allocateAndSeek(DATA_TYPE.ROOT_PAGE, 0);
			int rootPage2 = out.allocateAndSeek(DATA_TYPE.ROOT_PAGE, 0);

			//header: this is written further down
			
			//write User data
			int userData = out.allocateAndSeek(DATA_TYPE.USERS, 0);
			
			
			//dir for schemata
			int schemaData = out.allocateAndSeekAP(DATA_TYPE.SCHEMA_INDEX, 0, -1);
			//ID of next page
			out.writeInt(0);
			//Schema ID / schema data (page or actual data?)
			//0 for no more schemata
			out.writeInt(0);

			
			//dir for indices
			int indexDirPage = out.allocateAndSeek(DATA_TYPE.INDEX_MGR, 0);
			//ID of next page
			out.writeInt(0);
			//Schema ID / attribute ID / index type / Page ID
			//0 for nor more indices
			out.writeInt(0);

			//OID index
			PagedOidIndex oidIndex = new PagedOidIndex(file);
//			bootstrapSchema(raf, oidIndex);
			int oidPage = oidIndex.write();

			//Free space index
			int freeSpacePg = fsm.write();
			
			//write header
			out.seekPageForWrite(DATA_TYPE.DB_HEADER, headerPage);
			out.writeInt(DB_FILE_TYPE_ID);
			out.writeInt(DB_FILE_VERSION_MAJ);
			out.writeInt(DB_FILE_VERSION_MIN);
			out.writeInt(ZooConfig.getFilePageSize());
			out.writeInt(rootPage1);
			out.writeInt(rootPage2);
			
			writeRoot(out, rootPage1, 1, userData, oidPage, schemaData, indexDirPage, freeSpacePg, 
					fsm.getPageCount());
			writeRoot(out, rootPage2, 0, userData, oidPage, schemaData, indexDirPage, freeSpacePg, 
					fsm.getPageCount());
			
			
			
			file.close();
			file = null;
			out = null;


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
		} catch (IOException e) {
			e.printStackTrace();
			throw DBLogger.newUser("ERROR While creating database: " + dbPath, e);
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (JDOException e) {
					e.printStackTrace();
					//ignore
				}
			}
		}
	}
	
//	private void bootstrapSchema(PageAccessFile raf, PagedOidIndex oidIndex) {
//		PagedObjectAccess poa = new PagedObjectAccess(raf, oidIndex, null);
//		DataSerializer ds = new DataSerializer(poa, null, null);
//		ZooClassDef zpc = ZooClassDef.bootstrapZooPCImpl();
//		ZooClassDef cd = ZooClassDef.bootstrapZooClassDef();
//		cd.associateFields();
//		cd.associateJavaTypes();
//		PCContext pcc = new PCContext(cd, null, null);
//		zpc.jdoZooInit(ObjectState.PERSISTENT_NEW, pcc, zpc.getOid());
//		cd.jdoZooInit(ObjectState.PERSISTENT_NEW, pcc, cd.getOid());
//		ds.writeObject(zpc, cd, zpc.getOid());
//		ds.writeObject(cd, cd, cd.getOid());
//	}
	
	private void writeRoot(StorageChannelOutput out, int pageID, int txID, int userPage, int oidPage, 
			int schemaPage, int indexPage, int freeSpaceIndexPage, int pageCount) {
		out.seekPageForWrite(DATA_TYPE.ROOT_PAGE, pageID);
		//txID
		out.writeLong(txID);
		//User table
		out.writeInt(userPage);
		//OID table
		out.writeInt(oidPage);
		//schemata
		out.writeInt(schemaPage);
		//indices
		out.writeInt(indexPage);
		//free space index
		out.writeInt(freeSpaceIndexPage);
		//page count
		out.writeInt(pageCount);
		//last used oid
		out.writeLong(100);
		//txID
		out.writeLong(txID);
	}
	
	@Override
	public void removeDb(String dbName) {
		File dbFile = new File(toPath(dbName));
		DBLogger.debugPrint(1, "Removing DB file: " + dbFile.getAbsolutePath());
		if (!dbFile.exists()) {
			throw DBLogger.newUser("ZOO: DB folder does not exist: " + dbFile);
		}
		if (!dbFile.delete()) {
			throw DBLogger.newUser("ZOO: Could not remove DB file: " + dbFile);
		}
	}
	
	/**
	 * Create a repository (directory/folder) to contain databases.
	 */
	private void createDbFolder(File dbDir) {
		if (dbDir.exists()) {
		    return;
			//throw new JDOUserException("ZOO: Repository exists: " + dbFolder);
		}
		boolean r = dbDir.mkdirs();
		if (!r) {
			throw DBLogger.newUser("Could not create folders: " + dbDir.getAbsolutePath());
		}
	}

    @Override
    public String getDefaultDbFolder() {
        return DEFAULT_FOLDER;
    }
    
	@Override
	public String getDbPath(String dbName) {
		return toPath(dbName);
	}

	@Override
	public boolean dbExists(String dbName) {
		String dbPath = toPath(dbName);
		File db = new File(dbPath);
		return db.exists();
	}
}
