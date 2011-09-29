package org.zoodb.jdo.api;




public interface DataStoreManager {

	/**
	 * Create a repository (directory/folder) to contain databases.
	 */
	public void createDbRepository();

	
	/**
	 * Create a database file.
	 * 
	 * @param dbName
	 */
	public void createDb(String dbName);
	
	public void removedDbRepository();
	
	public void removeDb(String dbName);
	
	public String getRepositoryPath();
	
	public String getDbPath(String dbName);

	public boolean dbExists(String dbName);

	public boolean repositoryExists();
}
