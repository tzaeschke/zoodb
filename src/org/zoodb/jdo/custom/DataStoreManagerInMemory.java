package org.zoodb.jdo.custom;

import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_TYPE_ID;
import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_VERSION_MAJ;
import static org.zoodb.jdo.internal.server.DiskAccessOneFile.DB_FILE_VERSION_MIN;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.api.Schema;
import org.zoodb.jdo.internal.Config;
import org.zoodb.jdo.internal.Serializer;
import org.zoodb.jdo.internal.User;
import org.zoodb.jdo.internal.server.PageAccessFileInMemory;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.stuff.BucketArrayList;

public class DataStoreManagerInMemory extends DataStoreManager {

	private static boolean VERBOSE = false;

	private static final String DB_REP_PATH = "InMemory"; 

	private static final Map<String, BucketArrayList<ByteBuffer>> map = 
		new HashMap<String, BucketArrayList<ByteBuffer>>();

	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	public void dsCreateDbFiles(String dbName) {
		String dbDirName = DB_REP_PATH + File.separator + dbName;
		verbose("Creating DB file: " + dbDirName);

		String dbPath = dsGetDbPath(dbName);
		map.put(dbPath, new BucketArrayList<ByteBuffer>());
		
		//create files
		PageAccessFileInMemory raf = null;

		//DB file
		raf = new PageAccessFileInMemory(dbPath, "rw", Config.getPageSize());

		int headerPage = raf.allocateAndSeek(false);
		if (headerPage != 0) {
			throw new JDOFatalDataStoreException("Header page = " + headerPage);
		}
		int rootPage1 = raf.allocateAndSeek(false);
		int rootPage2 = raf.allocateAndSeek(false);

		//header: this is written further down

		//write User data
		int userData = raf.allocateAndSeek(false);
		String uName = System.getProperty("user.name");
		User user = new User(1, uName, "", true, true, true, false);
		Serializer.serializeUser(user, raf);
		raf.writeInt(0); //ID of next user, 0=no more users


		//dir for schemata
		int schemaData = raf.allocateAndSeek(false);
		//ID of next page
		raf.writeInt(0);
		//Schema ID / schema data (page or actual data?)
		//0 for no more schemata
		raf.writeInt(0);


		//dir for indices
		int indexDirPage = raf.allocateAndSeek(false);
		//ID of next page
		raf.writeInt(0);
		//Schema ID / attribute ID / index type / Page ID
		//0 for nor more indices
		raf.writeInt(0);

		//OID index
		PagedOidIndex oidIndex = new PagedOidIndex(raf);
		int oidPage = oidIndex.write();


		//write header
		raf.seekPage(headerPage, false);
		raf.writeInt(DB_FILE_TYPE_ID);
		raf.writeInt(DB_FILE_VERSION_MAJ);
		raf.writeInt(DB_FILE_VERSION_MIN);
		raf.writeInt(Config.getPageSize());
		raf.writeInt(rootPage1);
		raf.writeInt(rootPage2);

		raf.seekPage(rootPage1, false);
		//write main directory (page IDs)
		//User table 
		raf.writeInt(userData);
		//OID table
		raf.writeInt(oidPage);
		//schemata
		raf.writeInt(schemaData);
		//indices
		raf.writeInt(indexDirPage);


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
	}

	public void dsRemoveDbFiles(String dbName) {
		if (map.remove(dbName) == null) { 
			throw new JDOUserException("DB does not exist: " + dbName);
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
	public void dsCreateDbRepository() {
		//nothing to do
		//		File repDir = new File(DB_REP_PATH);
		//		if (repDir.exists()) {
		//			throw new JDOUserException("ZOO: Repository exists: " + DB_REP_PATH);
		//		}
		//		boolean r = repDir.mkdir();
		//		if (!r) {
		//			throw new JDOUserException("Could not create repository: " + repDir.getAbsolutePath());
		//		}
	}


	/**
	 * Create a folder to contain database files.
	 * This requires an existing database repository.
	 * @param dbName
	 */
	@Override
	public void dsCreateDbFolder(String dbName) {
		//nothing to do
		//		File dbDir = new File(DB_REP_PATH + File.separator + dbName);
		//		verbose("Creating DB folder: " + dbDir.getAbsolutePath());
		//		if (dbDir.exists()) {
		//			throw new JDOUserException("ZOO: DB folder already exists: " + dbDir);
		//		}
		//		dbDir.mkdir();
	}


	public void dsRemovedDbRepository() {
		//nothing to do?
		//		File repDir = new File(DB_REP_PATH);
		////		if (!repDir.exists()) {
		////			throw new JDOUserException(
		////					"ZOO: Repository exists: " + DB_REP_PATH);
		////		}
		//		if (!repDir.delete()) {
		//			throw new JDOUserException("ZOO: Could not remove repository: " + DB_REP_PATH);
		//		}
	}

	public void dsRemoveDbFolder(String dbName) {
		// nothing to do
		//		File dbDir = new File(DB_REP_PATH + File.separator + dbName);
		//		verbose("Removing DB folder: " + dbDir.getAbsolutePath());
		//		if (!dbDir.exists()) {
		//			throw new JDOUserException("ZOO: DB does not exist: " + dbDir);
		////			DatabaseLogger.debugPrintln(1, "Cannot remove DB folder since it does not exist: " + dbDir);
		////			return;
		//		}
		//		if (!dbDir.delete()) {
		//			throw new JDOUserException("ZOO: Could not remove DB folder: " + dbDir);
		//		}
	}

	@Override
	public String dsGetRepositoryPath() {
		return DB_REP_PATH;
	}

	@Override
	public String dsGetDbPath(String dbName) {
		return DB_REP_PATH + File.separator + dbName;
	}

	@Override
	public boolean dsDbExists(String dbName) {
		return map.containsKey(dbName);	}


	@Override
	public boolean dsRepositoryExists() {
		//File repDir = new File(DB_REP_PATH);
		//return repDir.exists();
		return true;
	}

	public static BucketArrayList<ByteBuffer> getInternalData(String dbPath) {
		return map.get(dbPath);
	}
}
