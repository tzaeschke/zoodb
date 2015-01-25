/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
