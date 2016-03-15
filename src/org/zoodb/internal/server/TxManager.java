/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import org.zoodb.internal.util.Pair;
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
 * In other words: a transaction can be removed if it ended before any CURRENTLY ACTIVE transaction 
 * started.
 * 
 * 
 * @author Tilmann Zaeschke
 */
class TxManager {

	/** This stores a history of all objects that were modified or deleted in a transaction. */
	private final PrimLongMapLI<ArrayList<TxObjInfo>> updateHistory = new PrimLongMapLI<>();
	
	private final PrimLongMapLI<TxObjInfo> updateSummary = new PrimLongMapLI<>();
	
	//TODO use CritBit tree?
	//Maps tx-end to tx-ID
	//Transactions are added in the order that they end. This means they are ordered by txEnd.
	private final LinkedList<Pair<Long, Long>> endedTx = new LinkedList<>();
	
	private boolean isSingleSession = true;
	
	private final LinkedList<Long> activeTXs = new LinkedList<>();
	private long latestTxId = -1;
	
	public TxManager(long txId) {
		this.latestTxId = txId;
	}
	
	/**
	 * Add updates of a transaction to the history tree.
	 * To be called when starting a new commit().
	 * @param txId
	 * @param txContext
	 * @return A list of conflicting objects or {@code null} if there are no conflicts
	 */
	synchronized List<Long> addUpdates(long txId, TxContext txContext, boolean isTrialRun) {
		if (isSingleSession) {
			//no need to record history
			return null;
		}
		
		ArrayList<TxObjInfo> updatesAndDeletes = txContext.getUpdatesAndDeletes();
		
		//first, check for conflicts
		ArrayList<Long> conflicts = null;
		for (TxObjInfo clientInfo: updatesAndDeletes) {
			long oid = clientInfo.getOid();
			long ots = clientInfo.getTS();
			//At this point we should not ignore objects that are apparently new!
			//Why? Even if the object appears new, the OID may be in use, which 
			//may present a conflict.

			TxObjInfo serverInfo = updateSummary.get(oid);
			//OLD:
			//Did the current transaction begin before the other was committed?
			//I.e. is the updateTimeStamp higher than the readTimeStamp of the current TX?
			//NEW: We just check whether the cached TS equals the expected TS. 
			//If not, we have a conflict. Note, that the new timestamp may be LOWER than the
			//cached timestamp if the object was updated AFTER the current TX started, but
			//before the current TX first accessed the object.
			//System.out.println("TxM-au: " + oid + " ots=" + ots + "   txTS=" + (serverInfo != null?serverInfo.getTS():"null"));
			if (serverInfo != null && serverInfo.getTxId() != ots) {
				if (clientInfo.isDeleted() && serverInfo.isDeleted()) {
					//okay, ignore
					//continue;
					//TODO For now we don't ignore these. To ignore these, we have to report back
					//so that there will be no attempts on updating any indexes. Furthermore,
					//it is not obvious that this is the right thing to do, because the TX may 
					//semantically rely on having deleted an object, however the object is already 
					//gone (for example if the number of deleted objects counts).
				}
				if (conflicts == null) {
					conflicts = new ArrayList<>();
				}
				conflicts.add(oid);
			}
		}
		if (conflicts != null || isTrialRun) {
			return conflicts;
		}
		
		//apply updates
		updateHistory.put(txId, updatesAndDeletes);
		for (TxObjInfo info: updatesAndDeletes) {
			// +1 to ensure conflicts even with latest transaction
			info.setTxId(txId);
			updateSummary.put(info.getOid(), info);
		}
		//not very clean: 'null' indicates no conflicts.
		return null;
	}
	
	/**
	 * Deregister the transaction.
	 * To be called after commit() or rollback().
	 * @param txId
	 */
	synchronized void deRegisterTx(long txId) {
		activeTXs.remove(txId);
		if (isSingleSession) {
			//no need to record history
			return;
		}
		
		//cache only if there are active TXs
		if (activeTXs.isEmpty()) {
			updateHistory.clear();
			updateSummary.clear();
			return;
		}
		
		endedTx.add(new Pair<>(latestTxId, txId));
		
		//drop all tx with END < min(active_start), i.e. drop all that ended before any of the 
		//remaining active transactions started.
		
		long minOpenTx = activeTXs.getFirst();
		while (!endedTx.isEmpty() && endedTx.getFirst().getA() < minOpenTx) {
			long beginID = endedTx.getFirst().getB();
			ArrayList<TxObjInfo> allUpdates = updateHistory.get(beginID);
			if (allUpdates != null) {
				for (TxObjInfo info: allUpdates) {
					long oid = info.getOid();
					long tx = updateSummary.get(oid).getTxId();
					if (tx == beginID) {
						updateSummary.remove(oid);
					}
				}
				updateHistory.remove(beginID);
			}
			endedTx.removeFirst();
		}
	}

	/**
	 * To be called when opening a new transaction.
	 * @return ID for the new TX
	 */
	synchronized long getNextTxId() {
		//this is synchronized to ensure correct ordering of the list.
		//alternatively we could use a sorted list, but this is probably not cheaper... ?
		latestTxId++;
		activeTXs.add(latestTxId);
		return latestTxId;
	}

	synchronized void setMultiSession() {
		isSingleSession = false;
	}
	
	synchronized int statsGetBufferedTxCount() {
		return updateHistory.size();
	}
	
	synchronized int statsGetBufferedOidCount() {
		return updateSummary.size();
	}
}
