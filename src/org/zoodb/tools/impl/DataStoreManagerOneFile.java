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
package org.zoodb.tools.impl;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import javax.jdo.JDOException;
import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zoodb.api.DBArrayList;
import org.zoodb.api.DBHashMap;
import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.SessionFactory;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.StorageRoot;
import org.zoodb.internal.server.StorageRootFile;
import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.schema.ZooSchema;
import org.zoodb.tools.ZooConfig;

public class DataStoreManagerOneFile implements DataStoreManager {

	public static final Logger LOGGER = LoggerFactory.getLogger(DataStoreManagerOneFile.class);

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
	 * @param dbName The database file name or path 
	 */
	@Override
	public void createDb(String dbName) {
	    String dbPath = toPath(dbName);
        LOGGER.info("Creating DB file: {}", dbPath);
        String folderPath = dbPath.substring(0, dbPath.lastIndexOf(File.separator));
        File dbDir = new File(folderPath);
        if (!dbDir.exists()) {
            createDbFolder(dbDir);
            LOGGER.info("Creating DB folder: {}", dbDir.getAbsolutePath());
        }

		
		//create files
		IOResourceProvider file = null;
		StorageRoot root = null;
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
			root = new StorageRootFile(dbPath, "rw",
					ZooConfig.getFilePageSize(), fsm);
			file = root.createChannel();
			StorageChannelOutput out = file.createWriter(false);
			fsm.initBackingIndexNew(file);
			fsm.notifyBegin(0);
			
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
//			bootstrapSchema(raf, oidIndex);
			int oidPage = file.writeIndex(oidIndex::write);

			//Free space index
			int freeSpacePg = file.writeIndex(fsm::write);
			
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
		out.seekPageForWrite(PAGE_TYPE.ROOT_PAGE, pageID);
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
        //commitID we simply use txId here)
        out.writeLong(txID);
	}
	
	@Override
	public boolean removeDb(String dbName) {
		File dbFile = new File(toPath(dbName));
		LOGGER.info("Removing DB file: {}", dbFile.getAbsolutePath());
		if (!dbFile.exists()) {
			return false;
			//throw DBLogger.newUser("ZOO: DB folder does not exist: " + dbFile);
		}
//		if (!dbFile.delete()) {
//			throw DBLogger.newUser("ZOO: Could not remove DB file: " + dbFile);
//		}
		SessionFactory.cleanUp(dbFile);
		return dbFile.delete();
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
