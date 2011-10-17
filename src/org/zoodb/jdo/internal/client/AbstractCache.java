package org.zoodb.jdo.internal.client;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public interface AbstractCache {

	public abstract boolean isSchemaDefined(Class<?> type, Node node);

	public abstract void rollback();

	public abstract void markPersistent(PersistenceCapableImpl pc, long oid, Node node);
	
	public abstract void addHollow(PersistenceCapableImpl obj, Node node, ZooClassDef classDef, 
	        long oid);
	
	public abstract void addPC(PersistenceCapableImpl obj, Node node, ZooClassDef classDef, 
	        long oid);

	public abstract CachedObject findCoByOID(long oid);

	public abstract ZooClassDef getSchema(long clsOid);

	public abstract ZooClassDef getSchema(Class<?> cls, Node node);

}
