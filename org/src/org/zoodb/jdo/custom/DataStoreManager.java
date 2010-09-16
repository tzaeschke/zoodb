package org.zoodb.jdo.custom;

import java.io.File;

import javax.jdo.JDOUserException;


public abstract class DataStoreManager {

	private static final DataStoreManager INSTANCE = new DataStoreManagerOneFile();
	
	private static boolean VERBOSE = false;
	
	private static final String DB_REP_PATH = 
		System.getProperty("user.home") + File.separator +
		File.separator + "zoodb"; 
	
	
	/**
	 * Create a repository (directory/folder) to contain databases.
	 */
	public static void createDbRepository() {
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
	public static void createDbFolder(String dbName) {
		File dbDir = new File(DB_REP_PATH + File.separator + dbName);
		verbose("Creating DB folder: " + dbDir.getAbsolutePath());
		if (dbDir.exists()) {
			throw new JDOUserException("ZOO: DB folder already exists: " + dbDir);
		}
		dbDir.mkdir();
	}

	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	public static void createDbFiles(String dbName) {
		INSTANCE.dsCreateDbFiles(dbName);
	}
	
	protected abstract void dsCreateDbFiles(String dbName);


	public static void removedDbRepository() {
		File repDir = new File(DB_REP_PATH);
//		if (!repDir.exists()) {
//			throw new JDOUserException(
//					"ZOO: Repository exists: " + DB_REP_PATH);
//		}
		if (!repDir.delete()) {
			throw new JDOUserException("ZOO: Could not remove repository: " + DB_REP_PATH);
		}
	}

	public static void removeDbFolder(String dbName) {
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

	public static void removeDbFiles(String dbName) {
		INSTANCE.dsRemoveDbFiles(dbName);
	}
	
	protected abstract void dsRemoveDbFiles(String dbName);


	public static String getRepositoryPath() {
		return DB_REP_PATH;
	}
	
	public static String getDbPath(String dbName) {
		return DB_REP_PATH + File.separator + dbName;
	}

	public static boolean dbExists(String dbName) {
		String dbDirName = getDbPath(dbName);
		
		File dbDir = new File(dbDirName);
		if (!dbDir.exists()) {
			return false;  //DB folder does not exist
		}

		
		//Check a file
		File oids = new File(dbDirName + File.separator + "OIDS");
		return oids.exists();
	}


	public static boolean repositoryExists() {
		File repDir = new File(DB_REP_PATH);
		return repDir.exists();
	}
	
	private static void verbose(String s) {
		if (VERBOSE) {
			System.out.println("DataStoreManager: " + s);
		}
	}
}
