package org.zoodb.jdo.internal.server;

import java.util.List;

import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public interface DiskAccess {
	
	public ZooClassDef readSchema(String clsName, ZooClassDef defSuper);
	
	public void writeSchema(ZooClassDef sch, boolean isNew, long oid);

	public void deleteSchema(ZooClassDef sch);
	
	public long[] allocateOids(int oidAllocSize);
	
	public void deleteObject(Object obj, long oid);

	public List<PersistenceCapableImpl> readAllObjects(String className, AbstractCache cache);
	
	public List<PersistenceCapableImpl> readObjects(long[] oids, AbstractCache cache);
	
	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	public PersistenceCapableImpl readObject(AbstractCache cache, long oid);
	
	public void close();

	public void postCommit();

	public void writeObjects(Class<?> key, List<CachedObject> value);
	
}
