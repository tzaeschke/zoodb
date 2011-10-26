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
import java.io.IOException;
import java.util.Properties;

import javax.jdo.JDOException;
import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.api.DataStoreManager;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.internal.Config;
import org.zoodb.jdo.internal.Serializer;
import org.zoodb.jdo.internal.User;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.PageAccessFile_BB;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.DatabaseLogger;

public class DataStoreManagerOneFile implements DataStoreManager {

	private static final String DB_REP_PATH = 
		System.getProperty("user.home") + File.separator + "zoodb"; 
	
	
	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	@Override
	public void createDb(String dbName) {
		File dbDir = new File(DB_REP_PATH);
		DatabaseLogger.debugPrint(1, "Creating DB file: " + dbDir.getAbsolutePath());
		if (!dbDir.exists()) {
			throw new JDOUserException("ZOO: DB repository does not exist: " + dbDir);
		}

		
		//create files
		PageAccessFile raf = null;
		try {
			//DB file
			File dbFile = new File(DB_REP_PATH + File.separator + dbName);
			if (dbFile.exists()) {
				throw new JDOUserException("ZOO: DB already exists: " + dbFile);
			}
			if (!dbFile.createNewFile()) {
				throw new JDOUserException("ZOO: Error creating DB file: " + dbFile);
			}
			FreeSpaceManager fsm = new FreeSpaceManager();
			raf = new PageAccessFile_BB(dbFile.getAbsolutePath(), "rw", Config.getFilePageSize(), 
					fsm);
			fsm.initBackingIndexNew(raf);
			
			int headerPage = raf.allocateAndSeek(false, 0);
			if (headerPage != 0) {
				throw new JDOFatalDataStoreException("Header page = " + headerPage);
			}
			int rootPage1 = raf.allocateAndSeek(false, 0);
			int rootPage2 = raf.allocateAndSeek(false, 0);

			//header: this is written further down
			
			//write User data
			int userData = raf.allocateAndSeek(false, 0);
            String uName = System.getProperty("user.name");
			User user = new User(1, uName, "", true, true, true, false);
			Serializer.serializeUser(user, raf);
			raf.writeInt(0); //ID of next user, 0=no more users
			
			
			//dir for schemata
			int schemaData = raf.allocateAndSeek(false, 0);
			//ID of next page
			raf.writeInt(0);
			//Schema ID / schema data (page or actual data?)
			//0 for no more schemata
			raf.writeInt(0);

			
			//dir for indices
			int indexDirPage = raf.allocateAndSeek(false, 0);
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
			raf.seekPageForWrite(headerPage, false);
			raf.writeInt(DB_FILE_TYPE_ID);
			raf.writeInt(DB_FILE_VERSION_MAJ);
			raf.writeInt(DB_FILE_VERSION_MIN);
			raf.writeInt(Config.getFilePageSize());
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
			
			ZooSchema.create(pm, PersistenceCapableImpl.class, dbName);
			ZooSchema.create(pm, DBHashtable.class, dbName);
			ZooSchema.create(pm, DBVector.class, dbName);
			
			pm.currentTransaction().commit();
			pm.close();
			pmf.close();
		} catch (IOException e) {
			throw new JDOUserException("ERROR While creating database.", e);
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (JDOException e) {
					e.printStackTrace();
					//ignore
				}
			}
		}
	}
	
	private void writeRoot(PageAccessFile raf, int pageID, int txID, int userPage, int oidPage, 
			int schemaPage, int indexPage, int freeSpaceIndexPage, int pageCount) {
		raf.seekPageForWrite(pageID, false);
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
		File dbFile = new File(DB_REP_PATH + File.separator + dbName);
		DatabaseLogger.debugPrint(1, "Removing DB file: " + dbFile.getAbsolutePath());
		if (!dbFile.exists()) {
			throw new JDOUserException("ZOO: DB folder does not exist: " + dbFile);
		}
		if (!dbFile.delete()) {
			throw new JDOUserException("ZOO: Could not remove DB file: " + dbFile);
		}
	}
	
	/**
	 * Create a repository (directory/folder) to contain databases.
	 */
	@Override
	public void createDbRepository() {
		File repDir = new File(DB_REP_PATH);
		if (repDir.exists()) {
			throw new JDOUserException("ZOO: Repository exists: " + DB_REP_PATH);
		}
		boolean r = repDir.mkdir();
		if (!r) {
			throw new JDOUserException("Could not create repository: " + repDir.getAbsolutePath());
		}
	}

	
	@Override
	public void removedDbRepository() {
		File repDir = new File(DB_REP_PATH);
//		if (!repDir.exists()) {
//			throw new JDOUserException(
//					"ZOO: Repository exists: " + DB_REP_PATH);
//		}
		if (!repDir.delete()) {
			throw new JDOUserException("ZOO: Could not remove repository: " + DB_REP_PATH);
		}
	}

	@Override
	public String getRepositoryPath() {
		return DB_REP_PATH;
	}
	
	@Override
	public String getDbPath(String dbName) {
		return DB_REP_PATH + File.separator + dbName ;
	}

	@Override
	public boolean dbExists(String dbName) {
		String dbPath = DB_REP_PATH + File.separator + dbName;
		
		File db = new File(dbPath);
		return db.exists();
	}


	@Override
	public boolean repositoryExists() {
		File repDir = new File(DB_REP_PATH);
		return repDir.exists();
	}
}
