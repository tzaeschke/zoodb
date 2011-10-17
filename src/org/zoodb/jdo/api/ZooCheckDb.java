package org.zoodb.jdo.api;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.zoodb.jdo.internal.Session;

public class ZooCheckDb {

	private static final String DB_NAME = "TestDb"; 
	//private static final String DB_NAME = "RandomRegularGraph-n1000-d20";
	//private static final String DB_NAME = "zoodb"; 

	public static void main(String[] args) {
		String dbName;
		if (args.length == 0) {
			dbName = DB_NAME;
		} else {
			dbName = args[0];
		}
		System.out.println("Checking database: " + dbName);

		ZooJdoProperties props = new ZooJdoProperties(dbName);
		PersistenceManagerFactory pmf = JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm = pmf.getPersistenceManager();
		Session s = (Session) pm.getDataStoreConnection().getNativeConnection();
		String report = s.getPrimaryNode().checkDb();
		System.out.println("Report");
		System.out.println("======");

		System.out.println(report);

		System.out.println("======");
		pm.close();
		pmf.close();
		System.out.println("Checking database done.");
	}

}
