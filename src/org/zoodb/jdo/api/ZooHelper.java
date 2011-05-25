package org.zoodb.jdo.api;

import java.lang.reflect.Constructor;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.Config;

public class ZooHelper {

	private static DataStoreManager INSTANCE = null;

	public static DataStoreManager getDataStoreManager() {
		if (INSTANCE != null && Config.getFileManager().equals(INSTANCE.getClass().getName())) {
			return INSTANCE;
		}
		
		//create a new one
		try {
			Class<?> cls = Class.forName(Config.getFileManager());
			Constructor<?> con = (Constructor<?>) cls.getConstructor();
			DataStoreManager dsm = (DataStoreManager) con.newInstance();
			INSTANCE = dsm;
			return dsm;
		} catch (Exception e) {
			throw new JDOFatalDataStoreException("", e);
		}
	}
}
