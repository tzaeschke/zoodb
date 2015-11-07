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

public class TxObjInfo {

	private final long oid;
	private final long timestamp;
	private final boolean deleted;
	private long txId;
	
	public TxObjInfo(long oid, long ts, boolean deleted) {
		this.oid = oid;
		this.timestamp = ts;
		this.deleted = deleted;
	}

	public long getOid() {
		return oid;
	}
	
	public long getTS() {
		return timestamp;
	}
	
	public boolean isDeleted() {
		return deleted;
	}

	void setTxId(long txId) {
		this.txId = txId;
	}
	
	long getTxId() {
		return txId;
	}
	
}
