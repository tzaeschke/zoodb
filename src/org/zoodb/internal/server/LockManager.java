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

import java.util.function.Supplier;

public interface LockManager {

	public static final LockManager NULL = null;
	
	public static LockManager DUMMY = new LockManager() {
		
		@Override
		public void assertWLock() {
			//Nothing
		}
		
		@Override
		public void assertRLock() {
			//Nothing
		}
		
		@Override
		public <T> T assertLocked(Supplier<T> r) {
			//Nothing
			return null;
		}
		
		@Override
		public void assertLocked(Runnable r) {
			//Nothing
		}
		
		@Override
		public String toString() {
			return "DUMMY";
		}
	};

	void assertLocked(Runnable r);
	<T> T assertLocked(Supplier<T> r);
	void assertRLock();
	void assertWLock();
	
}
