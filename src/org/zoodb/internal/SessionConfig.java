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
package org.zoodb.internal;

import org.zoodb.internal.util.DBLogger;

public class SessionConfig {

	private boolean isFrozen = false;

	private boolean isAutoCreateSchema = true;
	private boolean isEvictPrimitives = false;
	private boolean isDetachAllOnCommit = false;
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

	public CACHE_MODE getCacheMode() {
		return cacheMode;
	}

	public void setCacheMode(CACHE_MODE cacheMode) {
		checkFrozen();
		this.cacheMode = cacheMode;
	}

}
