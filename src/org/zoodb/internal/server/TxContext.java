/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
	private final ArrayList<Long> updatesAndDeleteOids = new ArrayList<>();
	private final ArrayList<Long> updatesAndDeleteTimestamps = new ArrayList<>();
	
	//Final! If this changes then we have a incompatibility anyway (except for generic objects)
	//So we don'allow this to change.
	//Also, schema evolution should only work in single-session mode.
	//--> Possible exception: creation of indexes...
	private long classSchemaTxId; 
	
	void setSchemaTxId(long schemaTxId) {
		this.classSchemaTxId = schemaTxId;
	}
	
	long getSchemaTxId() {
		return this.classSchemaTxId;
	}
	
	ArrayList<Long> getUpdatesAndDeleteOids() {
		return updatesAndDeleteOids;
	}

	ArrayList<Long> getUpdatesAndDeleteTimeStamps() {
		return updatesAndDeleteTimestamps;
	}

	void addOidUpdate(long oid, long readTimestamp) {
		updatesAndDeleteOids.add(oid);
		updatesAndDeleteTimestamps.add(readTimestamp);
	}

	void addOidUpdates(ArrayList<Long> oids, ArrayList<Long> updateTimestamps) {
		updatesAndDeleteOids.addAll(oids);
		updatesAndDeleteTimestamps.addAll(updateTimestamps);
	}

	void reset() {
		updatesAndDeleteOids.clear();
		updatesAndDeleteTimestamps.clear();
	}	
	
}
