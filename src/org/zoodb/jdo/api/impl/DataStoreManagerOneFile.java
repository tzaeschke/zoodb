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
import org.zoodb.jdo.api.Schema;
import org.zoodb.jdo.api.ZooJdoProperties;
import org.zoodb.jdo.internal.Config;
import org.zoodb.jdo.internal.Serializer;
import org.zoodb.jdo.internal.User;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.PageAccessFile_BB;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class DataStoreManagerOneFile implements DataStoreManager {

	private static boolean VERBOSE = false;
	
	private static final String DB_FILE_NAME = "zoo.db";
	private static final String DB_REP_PATH = 
		System.getProperty("user.home") + File.separator + "zoodb"; 
	
	
	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	@Override
	public void createDbFiles(String dbName) {
		String dbDirName = DB_REP_PATH + File.separator + dbName;
		verbose("Creating DB file: " + dbDirName);
		
		File dbDir = new File(dbDirName);
		if (!dbDir.exists()) {
			throw new JDOUserException("ZOO: DB folder does not exist: " + dbDir);
		}

		
		//create files
		PageAccessFile raf = null;
		try {
			//DB file
			File dbFile = new File(dbDirName + File.separator + DB_FILE_NAME);
			if (!dbFile.createNewFile()) {
				throw new JDOUserException("ZOO: DB folder already contains DB file: " + dbFile);
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
			
			Schema.create(pm, PersistenceCapableImpl.class, dbName);
			Schema.create(pm, DBHashtable.class, dbName);
			Schema.create(pm, DBVector.class, dbName);
			
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
	public void removeDbFiles(String dbName) {
		File dbDir = new File(DB_REP_PATH + File.separator + dbName);
		verbose("Creating DB files: " + dbDir.getAbsolutePath());
		if (!dbDir.exists()) {
			throw new JDOUserException("ZOO: DB folder does not exist: " + dbDir);
		}
		File[] files = dbDir.listFiles();
		if (files.length == 0) {
			throw new JDOUserException("ZOO: DB folder is empty: " + dbDir);
		}
		for (File f: files) {
			removeFile(f);
		}
	}
	
	private static void removeFile(File file) {
		//if it is a directory, first remove the content
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File f: files) {
				removeFile(f);
			}
		}
		//now remove the file/directory itself
		if (!file.delete()) {
			throw new JDOUserException("ZOO: Could not remove DB file: " + file);
		}
	}
	
	private static void verbose(String s) {
		if (VERBOSE) {
			System.out.println("DataStoreManager: " + s);
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

	
	/**
	 * Create a folder to contain database files.
	 * This requires an existing database repository.
	 * @param dbName
	 */
	@Override
	public void createDbFolder(String dbName) {
		File dbDir = new File(DB_REP_PATH + File.separator + dbName);
		verbose("Creating DB folder: " + dbDir.getAbsolutePath());
		if (dbDir.exists()) {
			throw new JDOUserException("ZOO: DB folder already exists: " + dbDir);
		}
		if (!dbDir.mkdir()) {
			throw new JDOUserException("ZOO: Could not create DB folder: " + dbDir);
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
	public void removeDbFolder(String dbName) {
		File dbDir = new File(DB_REP_PATH + File.separator + dbName);
		verbose("Removing DB folder: " + dbDir.getAbsolutePath());
		if (!dbDir.exists()) {
			throw new JDOUserException("ZOO: DB does not exist: " + dbDir);
			//TODO throw Exception?
//			DatabaseLogger.debugPrintln(1, "Cannot remove DB folder since it does not exist: " + dbDir);
//			return;
		}
		if (!dbDir.delete()) {
			throw new JDOUserException("ZOO: Could not remove DB folder: " + dbDir);
		}
	}

	@Override
	public String getRepositoryPath() {
		return DB_REP_PATH;
	}
	
	@Override
	public String getDbPath(String dbName) {
		return DB_REP_PATH + File.separator + dbName + File.separator + DB_FILE_NAME;
	}

	@Override
	public boolean dbExists(String dbName) {
		String dbDirName = DB_REP_PATH + File.separator + dbName;
		
		File dbDir = new File(dbDirName);
		if (!dbDir.exists()) {
			return false;  //DB folder does not exist
		}

		
		//Check the file
		File db = new File(dbDirName + File.separator + DB_FILE_NAME);
		return db.exists();
	}


	@Override
	public boolean repositoryExists() {
		File repDir = new File(DB_REP_PATH);
		return repDir.exists();
	}
}
