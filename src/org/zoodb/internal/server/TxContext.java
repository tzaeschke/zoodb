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
package org.zoodb.internal.server;

import java.util.ArrayList;

/**
 * This class buffers all information of a transaction that needs to be applied to the database
 * infrastructure:
 * 
 * - Index updates
 * - object updates
 * 
 * 
 * @author Tilmann Zaeschke
 */
class TxContext {

	//TODO implement PrimLongArrayList?
	private final ArrayList<TxObjInfo> updatesAndDeletes = new ArrayList<>();
	
	//Final! If this changes then we have a incompatibility anyway (except for generic objects)
	//So we don'allow this to change.
	//Also, schema evolution should only work in single-session mode.
	//--> Possible exception: creation of indexes...
	private long classSchemaTxId; 
	private long classSchemaIndexTxId; 
	
	void setSchemaTxId(long schemaTxId) {
		this.classSchemaTxId = schemaTxId;
	}
	
	long getSchemaTxId() {
		return this.classSchemaTxId;
	}
	
	void setSchemaIndexTxId(long schemaIndexTxId) {
		this.classSchemaIndexTxId = schemaIndexTxId;
	}
	
	long getSchemaIndexTxId() {
		return this.classSchemaIndexTxId;
	}
	
	ArrayList<TxObjInfo> getUpdatesAndDeletes() {
		return updatesAndDeletes;
	}

	void addOidUpdates(ArrayList<TxObjInfo> uads) {
		updatesAndDeletes.addAll(uads);
	}

	void reset() {
		updatesAndDeletes.clear();
	}	
	
}
