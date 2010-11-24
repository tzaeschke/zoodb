package org.zoodb.jdo.custom;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.zip.CRC32;

import javax.jdo.JDOException;
import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.api.DBHashtable;
import org.zoodb.jdo.api.DBVector;
import org.zoodb.jdo.api.Schema;
import org.zoodb.jdo.internal.server.PageAccessFile;
import org.zoodb.jdo.internal.server.PageAccessFile_BB;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import static org.zoodb.jdo.internal.server.DiskAccessOneFile.*;

public class DataStoreManagerOneFile extends DataStoreManager {

	private static boolean VERBOSE = false;
	
	private static final String DB_REP_PATH = 
		System.getProperty("user.home") + File.separator + File.separator + "zoodb"; 
	
	
	/**
	 * Create database files.
	 * This requires an existing database folder.
	 * @param dbName
	 */
	public void dsCreateDbFiles(String dbName) {
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
			File dbFile = new File(dbDirName + File.separator + "zoo.db");
			if (!dbFile.createNewFile()) {
				throw new JDOUserException("ZOO: DB folder already contains DB file: " + dbFile);
			}
			raf = new PageAccessFile_BB(dbFile, "rw");
			
			int headerPage = raf.allocateAndSeek(false);
			if (headerPage != 0) {
				throw new JDOFatalDataStoreException("Header page = " + headerPage);
			}
			int rootPage1 = raf.allocateAndSeek(false);
			int rootPage2 = raf.allocateAndSeek(false);
			
//			//write header
//			raf.writeInt(DB_FILE_TYPE_ID);
//			raf.writeInt(DB_FILE_VERSION_MAJ);
//			raf.writeInt(DB_FILE_VERSION_MIN);
//			raf.writeInt(DB_FILE_POS_MAIN_DIR);
//			raf.seekPage(0, DB_FILE_POS_MAIN_DIR);
//			
//			//write main directory (page IDs)
//			//User table 
//			raf.writeInt(1);
//			//OID table
//			raf.writeInt(0);
//			//schemata
//			raf.writeInt(3);
//			//indices
//			raf.writeInt(4);
//			//write points
//			raf.writeInt(5);
			
			
			//write User data
			int userData = raf.allocateAndSeek(false);
			raf.writeInt(1); //Interal user ID
			raf.writeBoolean(true);// DBA=yes
			raf.writeBoolean(true);// read access=yes
			raf.writeBoolean(true);// write access=yes
			raf.writeBoolean(false);// passwd=no
			String uName = System.getProperty("user.name");
			raf.writeString(uName);
			//use very simple XOR encryption to avoid password showing up in clear text.
			String passwd = "";
			CRC32 pwd = new CRC32();
			for (int i = 0; i < passwd.length(); i++) {
				pwd.update(passwd.charAt(i));
			}
			pwd.getValue();
			raf.writeLong(pwd.getValue()); //password
			
			raf.writeInt(0); //ID of next user, 0=no more users
			
//			User user = new User(System.getProperty("user.name"));
//			user.setDBA(true);
//			user.setPassword("");
//			user.setPasswordRequired(false);
//			user.setRW(true);
//			Serializer.serializeUser(user, out);

			
			
//			//write OIDs
//			int oidData = raf.allocateAndSeek();
//			//current max ID.
////			raf.writeLong(100); //total
//			raf.writeInt(0); //IDs on this page
//			//following page, 0 for last page
//			raf.writeInt(0); 
//			raf.writeLong(0); //dummy
//			raf.writeLong(0); //list of allocated OIDs
			
			
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

			//write points page
//			int writePointPage = raf.allocateAndSeek();
//			raf.writeInt(10);  //last allocated page 
//			raf.writeInt(6);  //schema Index 
//			raf.writeInt(7);  //OID Index 
//			raf.writeInt(8);  //data Index 
//			raf.writeInt(9);  //schema data 
//			raf.writeInt(10);  //normal data
//			//extend file length
//			raf.seekPage(10);
//			raf.writeInt(0);
//			

//			raf.flush();
//			raf.close();
//			raf = new PageAccessFile_BB(dbFile, "rw");
			
			PagedOidIndex oidIndex = new PagedOidIndex(raf);
			int oidPage = oidIndex.write();

//			raf.flush();
			
			//write header
			raf.seekPage(headerPage, false);
			raf.writeInt(DB_FILE_TYPE_ID);
			raf.writeInt(DB_FILE_VERSION_MAJ);
			raf.writeInt(DB_FILE_VERSION_MIN);
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
			
//			Node1P node = new Node1P(dbName);
//			DiskAccess disk = new DiskAccess(node);
//			Schema sch = new Schema1P(DBHashtable.class, 10, true); //true, because it has an OID
//			disk.writeSchema(sch, true);
//			sch = new Schema1P(DBVector.class, 11, true); //true, because it has an OID
//			disk.writeSchema(sch, true);
			Schema.create(pm, PersistenceCapableImpl.class, dbName);
			Schema.create(pm, DBHashtable.class, dbName);
			Schema.create(pm, DBVector.class, dbName);
			
			pm.currentTransaction().commit();
			pm.close();
			pmf.close();
		} catch (IOException e) {
			throw new JDOUserException("ERROR While creating database.", e);
//		} catch (Throwable t) {
//			t.printStackTrace();
//			throw new JDOFatalDataStoreException("ERROR While creating database.", t);
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
	
	public void dsRemoveDbFiles(String dbName) {
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
}
