package org.zoodb.jdo.internal.client;

import javax.jdo.ObjectState;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public interface AbstractCache {

	public abstract boolean isSchemaDefined(Class<?> type, Node node);

	public abstract void rollback();

	public abstract void markPersistent(PersistenceCapableImpl pc, long oid, Node node, 
			ZooClassDef clsDef);
	
	public abstract PersistenceCapableImpl findCoByOID(long oid);

	public abstract ZooClassDef getSchema(long clsOid);

	public abstract ZooClassDef getSchema(Class<?> cls, Node node);

	public abstract void addToCache(PersistenceCapableImpl obj,
			ZooClassDef classDef, long oid, ObjectState state);

}
