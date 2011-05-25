package org.zoodb.jdo.api;




public interface DataStoreManager {

	/**
	 * Create a repository (directory/folder) to contain databases.
	 */
	public void createDbRepository();

	
	/**
	 * Create a folder to contain database files.
	 * This requires an existing database repository.
	 * @param dbName
	 */
	public void createDbFolder(String dbName);

	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	public void createDbFiles(String dbName);
	
	public void removedDbRepository();
	
	public void removeDbFolder(String dbName);

	public void removeDbFiles(String dbName);
	
	public String getRepositoryPath();
	
	public String getDbPath(String dbName);

	public boolean dbExists(String dbName);

	public boolean repositoryExists();
}
