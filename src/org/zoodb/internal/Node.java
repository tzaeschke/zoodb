/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal;

import java.util.ArrayList;
import java.util.Iterator;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.server.OptimisticTransactionResult;
import org.zoodb.internal.server.TxObjInfo;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.tools.DBStatistics.STATS;

public abstract class Node {

	private String dbPath;
	
	protected Node(String dbPath) {
		this.dbPath = dbPath;
	}
	
	public final String getDbPath() {
		return dbPath;
	}

	public abstract OidBuffer getOidBuffer();

	public abstract void makePersistent(ZooPC obj);

	public abstract void commit();

	public abstract CloseableIterator<ZooPC> loadAllInstances(ZooClassProxy def, 
            boolean loadFromCache);

	public abstract ZooPC loadInstanceById(long oid);

	public abstract void closeConnection();

	public abstract void defineIndex(ZooClassDef def, ZooFieldDef f, boolean isUnique);

	public abstract boolean removeIndex(ZooClassDef def, ZooFieldDef f);

	public void connect() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//
	}

	public abstract Iterator<ZooPC> readObjectFromIndex(ZooFieldDef field, 
			long minValue, long maxValue, boolean loadFromCache);

	public abstract long getStats(STATS stats);

    public abstract String checkDb();

	public abstract void dropInstances(ZooClassProxy def);

	public abstract void defineSchema(ZooClassDef def);

	public abstract void renameSchema(ZooClassDef def, String newName);

	public abstract void newSchemaVersion(ZooClassDef defNew);

	public abstract void undefineSchema(ZooClassProxy def);

	public abstract void refreshObject(ZooPC pc);

	public abstract void refreshSchema(ZooClassDef def);

	public abstract long getSchemaForObject(long oid);

    public abstract DataSink createDataSink(ZooClassDef def);
    
    public abstract DataDeleteSink createDataDeleteSink(ZooClassDef clsDef);

	public abstract void revert();

	public abstract Session getSession();

    public abstract CloseableIterator<ZooHandleImpl> oidIterator(ZooClassProxy zooClassProxy, 
            boolean subClasses);

	public abstract long countInstances(ZooClassProxy clsDef, boolean subClasses);

	public abstract GenericObject readGenericObject(ZooClassDef def, long oid);
	
	public abstract boolean checkIfObjectExists(long oid);

	public abstract long beginTransaction();

	public abstract OptimisticTransactionResult rollbackTransaction();

	public abstract OptimisticTransactionResult beginCommit(ArrayList<TxObjInfo> updates);

	public abstract OptimisticTransactionResult checkTxConsistency(ArrayList<TxObjInfo> updates);

}
   