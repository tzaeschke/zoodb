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
	//private final List<Long> resolvedDeletionConflicts;
	
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
