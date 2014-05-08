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
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.zoodb.internal.util.PrimLongMapLI;

/**
 * Here we keep a history of modified objects in order to quickly detect conflicts.
 * 
 * Deletes: 
 * We consider double-deletion of objects as conflict (TODO check JDO spec)
 * Reasons: The object may have been recreated, for example by creating an object
 * with a dedicated OID or by changing the OID of an object.
 * 
 * See:
 * http://etutorials.org/Programming/Java+data+objects/Chapter+15.+Optimistic+Transactions/15.1+Verification+at+Commit/
 * --> They claim that
 * - Double delete should not fail
 * - a failure causes automatic rollback(), however a refresh() is still required ?!?!?
 *   --> Depending on the restoreValues() flag.
 * 
 * 
 * Conflicting transactions:
 * Potentially conflicting transactions with a transaction X are any transactions that:
 * a) began before X committed
 * AND
 * b) committed after X began 
 * In other words: a transaction can be removed if ended before any CURRENTLY ACTIVE transaction 
 * started.
 * 
 * 
 * @author Tilmann Zaeschke
 */
public class TxManager {

	/** This stores a history of all objects that were modified or deleted in a transaction. */
	private final PrimLongMapLI<ArrayList<Long>> updateHistory = new PrimLongMapLI<>();
	
	private final PrimLongMapLI<Long> updateSummary = new PrimLongMapLI<Long>();
	
	//TODO use CritBit tree?
	private final SortedMap<Long, Long> cachedTx = new TreeMap<>();
	
	private boolean isSingleSession = true;
	
	private final LinkedList<Long> activeTXs = new LinkedList<>();
	private long latestTxId = -1;
	
	public TxManager(SessionManager sm, long txId) {
		this.latestTxId = txId;
	}
	
	/**
	 * Add updates of a transaction to the history tree.
	 * @param txId
	 * @param txContext
	 * @return A list of conflicting objects or {@code null} if there are no conflicts
	 */
	synchronized List<Long> addUpdates(long txId, TxContext txContext) {
		if (isSingleSession) {
			//no need to record history
			return null;
		}
		
		ArrayList<Long> updatesAndDeleteOids = txContext.getUpdatesAndDeleteOids();
		ArrayList<Long> updatesAndDeleteTSs = txContext.getUpdatesAndDeleteTimeStamps();
		
		//first, check for conflicts
		ArrayList<Long> conflicts = null;
		for (int i = 0; i < updatesAndDeleteOids.size(); i++) {
			long oid = updatesAndDeleteOids.get(i);
			Long txTimestamp = updateSummary.get(oid);
			//OLD:
			//Did the current transaction begin before the other was comitted?
			//I.e. is the updateTimeStamp higher than the readTimeStamp of the current TX?
			//NEW: We just check whether the cached TS equals the expected TS. 
			//If not, we have a conflict. Note, that the new timestamp may be LOWER than the
			//cached timestamp if the object was updated AFTER the current TX started, but
			//before the current TX first accessed the object.
			if (txTimestamp != null && txTimestamp != updatesAndDeleteTSs.get(i)) {
				if (conflicts == null) {
					conflicts = new ArrayList<>();
				}
				conflicts.add(oid);
			}
		}
		if (conflicts != null) {
			return conflicts;
		}
		
		//apply updates
		updateHistory.put(txId, updatesAndDeleteOids);
		for (long oid: updatesAndDeleteOids) {
			// +1 to ensure conflicts even with latest transaction
			updateSummary.put(oid, txId);
		}
		return null;
	}
	
	synchronized void registerTx(long txId) {
		//append tx to end, even for single-tx in case other TXs join later.
		//Ordering is important!
		activeTXs.add(txId);
	}
	
	synchronized void deRegisterTx(long txId) {
		activeTXs.remove(txId);
		if (isSingleSession) {
			//no need to record history
			return;
		}
		
		//cache only if there are active TXs
		if (!activeTXs.isEmpty()) {
			//map begin->end
			cachedTx.put(txId, latestTxId);
			//TODO ???
		}
		
		//drop all tx with END < min(active_start), i.e. drop all that ended before any of the 
		//remaining active transactions started.
		
		
		ArrayList<Long> allUpdates = updateHistory.get(txId);
	}

	synchronized long getNextTxId() {
		//this is synchronized to ensure correct ordering of the list.
		//alternatively we could use a sorted list, but this is probably not cheaper... ?
		latestTxId++;
		activeTXs.add(latestTxId);
		return latestTxId;
	}

	void setMultiSession() {
		isSingleSession = false;
	}
}
