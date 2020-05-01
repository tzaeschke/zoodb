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
package org.zoodb.internal;

import org.zoodb.internal.server.SessionFactory;
import org.zoodb.internal.util.DBLogger;

public class SessionConfig {

	private boolean isFrozen = false;

	private boolean isAutoCreateSchema = true;
	private boolean isEvictPrimitives = false;
	private boolean failOnClosedQueries = false;
	private boolean isDetachAllOnCommit = false;
	private boolean isNonTransactionalRead = false;
	private CACHE_MODE cacheMode = CACHE_MODE.SOFT;


	/**
	 * Specifies whether persistent objects are reference from the client cache via wek references,
	 * soft references or strong (normal) references. 
	 * @author Tilmann Zaeschke
	 */
	public enum CACHE_MODE {
		WEAK,
		SOFT,
		PIN
	}
	
	private void checkFrozen() {
		if (isFrozen) {
			throw DBLogger.newUser("Session configuration cannot be changed after at this point.");
		}
	}
	
	public boolean getAutoCreateSchema() {
		return isAutoCreateSchema;
	}

	public void setAutoCreateSchema(boolean isAutoCreateSchema) {
		checkFrozen();
		this.isAutoCreateSchema = isAutoCreateSchema;
	}

	public boolean getDetachAllOnCommit() {
		return isDetachAllOnCommit;
	}

	public void setDetachAllOnCommit(boolean isDetachAllOnCommit) {
		checkFrozen();
		this.isDetachAllOnCommit = isDetachAllOnCommit;
	}

	public boolean getEvictPrimitives() {
		return isEvictPrimitives;
	}

	public void setEvictPrimitives(boolean isEvictPrimitives) {
		checkFrozen();
		this.isEvictPrimitives = isEvictPrimitives;
	}

	public boolean getFailOnClosedQueries() {
		return failOnClosedQueries;
	}

	public void setFailOnCloseQueries(boolean failOnClosedQueries) {
		this.failOnClosedQueries = failOnClosedQueries;
	}

	public CACHE_MODE getCacheMode() {
		return cacheMode;
	}

	public void setCacheMode(CACHE_MODE cacheMode) {
		checkFrozen();
		this.cacheMode = cacheMode;
	}

	public boolean getNonTransactionalRead() {
		return isNonTransactionalRead;
	}

	public void setNonTransactionalRead(boolean flag) {
		this.isNonTransactionalRead = flag;
		if (flag) {
			if (SessionFactory.MULTIPLE_SESSIONS_ARE_OPEN) {
				throw DBLogger.newFatal("Not supported: Can't use non-transactional read with "
						+ "mutliple sessions");
			}
			//TODO remove this once non-tx read is safe in multiple sessions
			SessionFactory.FAIL_BECAUSE_OF_ACTIVE_NON_TX_READ = true;
		}
	}
}
