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
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.zoodb.internal.util.PrimLongMapLI;

/**
 * Here we keep a history of modified objects in order to quickly detect conflicts.
 * 
 * Deletes: 
 * We consider double-deletion of objects as conflict (TODO check JDO spec)
 * Reasons: The object may have been recreated, for example by creating an object\
 * with a dedicated OID or by changing the OID of an object.
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
public class UpdateHistory {

	/** This stores a history of all objects that were modified or deleted in a transaction. */
	private final PrimLongMapLI<PrimLongMapLI<Long>> updateHistory = new PrimLongMapLI<>();
	
	private final PrimLongMapLI<Long> updateSummary = new PrimLongMapLI<Long>();
	
	private long minRequiredTxId = -1;
	
	private final ArrayList<Long> currentlyActiveTx = new ArrayList<Long>();
	//TODO use CritBit tree?
	private final SortedMap<Long, Long> cachedTx = new TreeMap<>();
	
	private boolean isSingleSession = true;
	
	private final SessionManager sm;
	
	public UpdateHistory(SessionManager sm) {
		this.sm = sm;
	}
	
	/**
	 * Add updates of a transaction to the history tree.
	 * @param txId
	 * @param updatesAndDeletes
	 * @return A list of conflicting objects or {@code null} if there are no conflicts
	 */
	List<Long> addUpdates(long txId, PrimLongMapLI<Long> updatesAndDeletes) {
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//TODO
		//we need to register much earlier, EVERY active transaction need to be known here!
		//Advantage: the list is automatically sorted!
		//OR: put List into Session manager?
		//Rename to TxManager?
		//--> Add to activeList when assigning txId (remove AtomicLong)
		
		
		//append tx to end, even for single-tx in case other TXs join later.
		currentlyActiveTx.add(txId);
		
		if (isSingleSession) {
			//no need to record history
			return null;
		}
		
		//first, check for conflicts
		ArrayList<Long> conflicts = null;
		for (long oid: updatesAndDeletes.keySet()) {
			if (updateSummary.containsKey(oid)) {
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
		updateHistory.put(txId, updatesAndDeletes);
		updateSummary.putAll(updatesAndDeletes);
		return null;
	}
	
	void deRegisterTx(long txId) {
		currentlyActiveTx.remove(txId);
		if (isSingleSession) {
			//no need to record history
			return;
		}
		
		//cache only if there are active TXs
		if (!currentlyActiveTx.isEmpty()) {
			long currentTx = sm.getCurrentTxId();
			cachedTx.put(txId, currentTx);
			//TODO ???
		}
		
		PrimLongMapLI<Long> allUpdates = updateHistory.get(txId);
	}
}
