package org.zoodb.jdo.internal;

import java.util.Date;
import java.util.Iterator;

import org.zoodb.jdo.custom.DataStoreManager;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

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
	
	public void rollback() {
		System.err.println("STUB: Node.rollback()");
	}

	public abstract void makePersistent(PersistenceCapableImpl obj);

	public abstract void commit();

	public abstract ZooClassDef loadSchema(String clsName, ZooClassDef defSuper);

	public abstract Iterator<PersistenceCapableImpl> loadAllInstances(Class<?> cls);

	public abstract PersistenceCapableImpl loadInstanceById(long oid);

	public abstract void closeConnection();

	public abstract void defineIndex(ZooClassDef def, ZooFieldDef f, boolean isUnique);

	public abstract boolean removeIndex(ZooClassDef def, ZooFieldDef f);

	public abstract byte readAttrByte(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract short readAttrShort(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract int readAttrInt(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract long readAttrLong(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract boolean readAttrBool(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract char readAttrChar(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract float readAttrFloat(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract double readAttrDouble(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract String readAttrString(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract Date readAttrDate(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public abstract long readAttrRefOid(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public void connect() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	public abstract Iterator<PersistenceCapableImpl> readObjectFromIndex(ZooClassDef clsDef, 
			ZooFieldDef field, long minValue, long maxValue, AbstractCache cache);
}
