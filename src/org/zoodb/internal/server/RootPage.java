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

final class RootPage {

	private int userPage;
	private int oidPage;
	private int schemaPage; 
	private int indexPage;
	private int freeSpaceIndexPage;
	private int pageCount;
	private long serverTime = 0;

	boolean isDirty(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage) {
		if (this.userPage != userPage || 
				this.oidPage != oidPage || 
				this.schemaPage != schemaPage ||
				this.indexPage != indexPage ||
				this.freeSpaceIndexPage != freeSpaceIndexPage) {
			return true;
		}
		return false;
	}
	
	void set(int userPage, int oidPage, int schemaPage, int indexPage, 
			int freeSpaceIndexPage, int pageCount) {
		this.userPage = userPage;
		this.oidPage = oidPage;
		this.schemaPage = schemaPage;
		this.indexPage = indexPage;
		this.freeSpaceIndexPage = freeSpaceIndexPage;
		this.pageCount = pageCount;
		this.serverTime++;
	}

	@Deprecated
	int getUserPage() {
		return userPage; 
	}

	/**
	 * 
	 * @return Index page.
	 * @deprecated This should probably be removed. Are we gonna use this at some point?
	 */
	int getIndexPage() {
		return indexPage;
	}

	public int getSchemIndexPage() {
		return schemaPage;
	}

	public int getOidIndexPage() {
		return oidPage;
	}

	public int getFMSPage() {
		return freeSpaceIndexPage;
	}

	public int getFSMPageCount() {
		return pageCount;
	}
	
	public long getLogicalServerTime() {
		return serverTime;
	}
}
