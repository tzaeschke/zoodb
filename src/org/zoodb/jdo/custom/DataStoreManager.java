package org.zoodb.jdo.custom;

import java.io.File;
import java.lang.reflect.Constructor;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.Config;



public abstract class DataStoreManager {

	private static DataStoreManager INSTANCE = new DataStoreManagerOneFile();
	
	private static DataStoreManager getInstance() {
		if (INSTANCE != null && Config.getFileManager().equals(INSTANCE.getClass().getName())) {
			return INSTANCE;
		}
		
		//create a new one
		try {
			Class<?> cls = Class.forName(Config.getFileManager());
			Constructor<?> con = (Constructor<?>) cls.getConstructor();
			DataStoreManager dsm = (DataStoreManager) con.newInstance();
			return dsm;
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}
	
	/**
	 * Create a repository (directory/folder) to contain databases.
	 */
	public static void createDbRepository() {
		getInstance().dsCreateDbRepository();
	}

	abstract void dsCreateDbRepository();

	
	/**
	 * Create a folder to contain database files.
	 * This requires an existing database repository.
	 * @param dbName
	 */
	public static void createDbFolder(String dbName) {
		getInstance().dsCreateDbFolder(dbName);
	}

	abstract void dsCreateDbFolder(String dbName);

	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	public static void createDbFiles(String dbName) {
		getInstance().dsCreateDbFiles(dbName);
	}
	
	protected abstract void dsCreateDbFiles(String dbName);


	abstract void dsRemovedDbRepository();
	
	public static void removedDbRepository() {
		getInstance().dsRemovedDbRepository();
	}

	abstract void dsRemoveDbFolder(String dbName);
	
	public static void removeDbFolder(String dbName) {
		getInstance().dsRemoveDbFolder(dbName);
	}

	public static void removeDbFiles(String dbName) {
		getInstance().dsRemoveDbFiles(dbName);
	}
	
	protected abstract void dsRemoveDbFiles(String dbName);


	abstract String dsGetRepositoryPath();
	
	public static String getRepositoryPath() {
		return getInstance().dsGetRepositoryPath();
	}
	
	abstract String dsGetDbPath(String dbName);
	
	public static String getDbPath(String dbName) {
		return getInstance().dsGetDbPath(dbName);
	}

	abstract boolean dsDbExists(String dbName);
	
	public static boolean dbExists(String dbName) {
		return getInstance().dsDbExists(dbName);
	}


	public static boolean repositoryExists() {
		return getInstance().dsRepositoryExists();
	}

	abstract boolean dsRepositoryExists();
}
