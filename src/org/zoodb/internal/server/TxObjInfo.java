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
