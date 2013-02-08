/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.jdo.internal.server;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.impl.DBStatistics.STATS;
import org.zoodb.jdo.internal.DataDeSerializer;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.ZooClassProxy;
import org.zoodb.jdo.internal.ZooFieldDef;
import org.zoodb.jdo.internal.ZooHandle;
import org.zoodb.jdo.internal.server.index.PagedOidIndex;
import org.zoodb.jdo.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.jdo.internal.util.CloseableIterator;

public interface DiskAccess {
	
	public void deleteSchema(ZooClassDef sch);
	
	public long[] allocateOids(int oidAllocSize);

	public CloseableIterator<ZooPCImpl> readAllObjects(long schemaOid, 
            boolean loadFromCache);
	
	/**
	 * Locate an object.
	 * @param oid
	 * @return Path name of the object (later: position of obj)
	 */
	public ZooPCImpl readObject(long oid);
	public ZooPCImpl readObject(DataDeSerializer dds, long oid);
	
	public void close();

	public void commit();

	/**
	 * Defines an index and populates it. All objects are put into the cache. This is not 
	 * necessarily useful, but it is a one-off operation. Otherwise we would need a special
	 * purpose implementation of the deserializer, which would have the need for a cache removed.
	 * @param cls
	 * @param field
	 * @param isUnique
	 * @param cache
	 */
	void defineIndex(ZooClassDef cls, ZooFieldDef field, boolean isUnique);

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

	/**
	 * WARNING: float/double values need to be converted with BitTools before used on indices. 
	 */
	Iterator<ZooPCImpl> readObjectFromIndex(ZooFieldDef field, 
			long minValue, long maxValue, boolean loadFromCache);

	public int getStats(STATS stats);

    public String checkDb();

	public void dropInstances(ZooClassDef def);

	public void defineSchema(ZooClassDef def);

	public void newSchemaVersion(ZooClassDef defOld, ZooClassDef defNew);

	public void undefineSchema(ZooClassDef def);

	public void readObject(ZooPCImpl pc);

	public void refreshSchema(ZooClassDef def);

	public void renameSchema(ZooClassDef def, String newName);

	public ZooClassDef readObjectClass(long oid);

    public SchemaIndexEntry getSchemaIE(long oid);

    public ObjectWriter getWriter(long clsOid);

    public PagedOidIndex getOidIndex();

	public void revert();

    public CloseableIterator<ZooHandle> oidIterator(ZooClassProxy px, boolean subClasses);
	
}
