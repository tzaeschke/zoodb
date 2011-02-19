package org.zoodb.jdo.internal;

import java.util.Collection;

import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.custom.DataStoreManager;

public abstract class Node {

	private String _dbPath;
	private String _url;
	
	protected Node(String url) {
		_url = url;
		_dbPath = DataStoreManager.getDbPath(url);
	}
	
	public final String getDbPath() {
		return _dbPath;
	}

	public abstract OidBuffer getOidBuffer();

	public Object getURL() {
		return _url;
	}

//	public abstract ISchema createSchema(Class cls, long oid, boolean isLoaded);
	
	public void rollback() {
		System.err.println("STUB: Node.rollback()");
	}

	public abstract void makePersistent(PersistenceCapableImpl obj);

	public abstract void commit();

	public abstract ZooClassDef loadSchema(String clsName, ZooClassDef defSuper);

//	public abstract ISchema locateSchema(Class cls);
//	
//	public abstract ISchema locateSchema(String className);
//	
//	public abstract boolean isSchemaDefined(Class type);

	public abstract Collection<?> loadAllInstances(Class<?> cls);

	public abstract PersistenceCapableImpl loadInstanceById(long oid);

	public abstract void closeConnection();

	public abstract void defineIndex(ZooClassDef def, ZooFieldDef f, boolean isUnique);

}
