package org.zoodb.jdo.internal.server;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.client.AbstractCache;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public interface DiskAccess {
	
	public ZooClassDef readSchema(String clsName, ZooClassDef defSuper);
	
	public void writeSchema(ZooClassDef sch, boolean isNew, long oid);

	public void deleteSchema(ZooClassDef sch);
	
	public long[] allocateOids(int oidAllocSize);
	
	public void deleteObject(Object obj, long oid);

	public Iterator<PersistenceCapableImpl> readAllObjects(String className, AbstractCache cache);
	
	public List<PersistenceCapableImpl> readObjects(long[] oids, AbstractCache cache);
	
	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	public PersistenceCapableImpl readObject(AbstractCache cache, long oid);
	
	public void close();

	public void postCommit();

	public void writeObjects(ZooClassDef clsDef, List<CachedObject> value, AbstractCache cache);

	/**
	 * Defines an index and populates it. All objects are put into the cache. This is not 
	 * necessarily useful, but it is a one-off operation. Otherwise we would need a special
	 * purpose implementation of the deserializer, which would have the need for a cache removed.
	 * @param cls
	 * @param field
	 * @param isUnique
	 * @param cache
	 */
	void defineIndex(ZooClassDef cls, ZooFieldDef field, boolean isUnique, AbstractCache cache);

	public boolean removeIndex(ZooClassDef def, ZooFieldDef field);

	public Collection<ZooClassDef> readSchemaAll();

	long readAttrLong(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	int readAttrInt(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	char readAttrChar(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	short readAttrShort(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	float readAttrFloat(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	double readAttrDouble(long oid, ZooClassDef schemaDef,
			ZooFieldDef attrHandle);

	boolean readAttrBool(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	byte readAttrByte(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public Date readAttrDate(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public String readAttrString(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	public long readAttrRefOid(long oid, ZooClassDef schemaDef, ZooFieldDef attrHandle);

	Iterator<PersistenceCapableImpl> readObjectFromIndex(ZooClassDef clsDef,
			ZooFieldDef field, long minValue, long maxValue, AbstractCache cache);
	
}
