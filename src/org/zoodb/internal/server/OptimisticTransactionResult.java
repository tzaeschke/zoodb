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

/**
 * Results of an optimistic verification.
 * 
 * @author Tilmann Zaeschke
 *
 */
public class OptimisticTransactionResult {

	private boolean isIndexRefreshNeeded;
	private boolean isSchemaResetNeeded;
	private final List<Long> conflicts;
	
	public OptimisticTransactionResult(List<Long> conflicts, boolean needsReset, 
			boolean needsRefresh) {
		isIndexRefreshNeeded = needsRefresh;
		isSchemaResetNeeded = needsReset;
		this.conflicts = conflicts;
	}

	public OptimisticTransactionResult() {
		// empty
		this.conflicts = new ArrayList<>();
	}

	public void add(OptimisticTransactionResult other) {
		isIndexRefreshNeeded |= other.isIndexRefreshNeeded;
		isSchemaResetNeeded |= other.isSchemaResetNeeded;
		if (other.conflicts != null) {
			conflicts.addAll(other.conflicts);
		}
	}

	public boolean hasFailed() {
		//do not consider schemaRefresh a failure!
		if (isSchemaResetNeeded || (conflicts != null && !conflicts.isEmpty())) {
			return true;
		}
		return false;
	}

	public boolean requiresReset() {
		return isSchemaResetNeeded;
	}

	public boolean requiresRefresh() {
		return isIndexRefreshNeeded;
	}

	public List<Long> getConflicts() {
		return conflicts;
	}

	public void setRefreshRequired(boolean b) {
		isIndexRefreshNeeded = b;
	}
	
	public void setResetRequired(boolean b) {
		isSchemaResetNeeded = b;
	}
	
}
