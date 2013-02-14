/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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
package org.zoodb.jdo.internal;

import java.util.Date;
import java.util.Iterator;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.jdo.api.impl.DBStatistics.STATS;
import org.zoodb.jdo.internal.util.CloseableIterator;
import org.zoodb.jdo.internal.util.DatabaseLogger;

public abstract class Node {

	private String dbPath;
	
	protected Node(String dbPath) {
		this.dbPath = dbPath;
	}
	
	public final String getDbPath() {
		return dbPath;
	}

	public abstract OidBuffer getOidBuffer();

	public void rollback() {
		//TODO
		DatabaseLogger.debugPrintln(2, "STUB: Node.rollback()");
		//System.err.println("STUB: Node.rollback()");
	}

	public abstract void makePersistent(ZooPCImpl obj);

	public abstract void commit();

	public abstract CloseableIterator<ZooPCImpl> loadAllInstances(ZooClassProxy def, 
            boolean loadFromCache);

	public abstract ZooPCImpl loadInstanceById(long oid);

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

	public abstract Iterator<ZooPCImpl> readObjectFromIndex(ZooFieldDef field, 
			long minValue, long maxValue, boolean loadFromCache);

	public abstract int getStats(STATS stats);

    public abstract String checkDb();

	public abstract void dropInstances(ZooClassDef def);

	public abstract void defineSchema(ZooClassDef def);

	public abstract void newSchemaVersion(ZooClassDef defOld, ZooClassDef defNew);

	public abstract void undefineSchema(ZooClassDef def);

	public abstract void refreshObject(ZooPCImpl pc);

	public abstract void refreshSchema(ZooClassDef def);

	public abstract void renameSchema(ZooClassDef def, String newName);

	public abstract long getSchemaForObject(long oid);

    public abstract DataSink createDataSink(ZooClassDef def);
    
    public abstract DataDeleteSink createDataDeleteSink(ZooClassDef clsDef);

	public abstract void revert();

	public abstract Session getSession();

    public abstract CloseableIterator<ZooHandleImpl> oidIterator(ZooClassProxy zooClassProxy, 
            boolean subClasses);

	public abstract long countInstances(ZooClassProxy clsDef, boolean subClasses);

	public abstract GenericObject readGenericObject(ZooClassDef def, long oid);
    
}
   