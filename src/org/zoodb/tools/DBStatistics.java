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
package org.zoodb.tools;

import org.zoodb.internal.Session;

/**
 * DB statistics API.
 * 
 * @author Tilmann Zaeschke
 */
public class DBStatistics {

	public enum STATS {
		/** Page read access counter. */
		IO_PAGE_READ_CNT,
		/** Page read access counter. Counts only unique access (each page counted only once). */
		IO_PAGE_READ_CNT_UNQ,
		/** Page write access counter. */
		IO_PAGE_WRITE_CNT,
		/** Data page (only stored objects) read access counter. */
		IO_DATA_PAGE_READ_CNT,
		/** Data page (only stored objects) read access counter. 
		 * Counts only unique access (each page counted only once). */
		IO_DATA_PAGE_READ_CNT_UNQ, 
		
		/** Number of pages used by free space manager. */
		DB_PAGE_CNT_IDX_FSM, 
		/** Number of pages used by OID index. */
		DB_PAGE_CNT_IDX_OID, 
		/** Number of pages used by POS index. */
		DB_PAGE_CNT_IDX_POS,
		/** Number of pages used by attribute indices. */
		DB_PAGE_CNT_IDX_ATTRIBUTES, 
		/** Number of pages used by data (serialised objects). */
		DB_PAGE_CNT_DATA,
		/** Total number of pages. */
		DB_PAGE_CNT, 
		
		/** Number of objects in buffered past transactions. */
		TX_MGR_BUFFERED_OID_CNT, 
		/** Number of buffered past transactions. */
		TX_MGR_BUFFERED_TX_CNT;
	}
	
	private final Session s;
	private static boolean ENABLED = false;
	
	public DBStatistics(Session s) {
		this.s = s;
	}

	/**
	 * 
	 * @return Number of read pages since the session was created.
	 */
	public int getStoragePageReadCount() {
		return s.getPrimaryNode().getStats(STATS.IO_PAGE_READ_CNT);
	}

	/**
	 * 
	 * @return Number of written pages since the session was created. This includes pages that 
	 * are not written yet (commit pending) and pages that have been rolled back.
	 */
	public int getStoragePageWriteCount() {
		return s.getPrimaryNode().getStats(STATS.IO_PAGE_WRITE_CNT);
	}

	/**
	 * Enabling statistics collection for ALL sessions.
	 * @param b enable/disable
	 */
	public static void enable(boolean b) {
		System.err.println("Warning: enabling stats for ALL sessions: " + b);
		ENABLED = b;
	}

	public static boolean isEnabled() {
		return ENABLED;
	}

	public int getStoragePageReadCountUnique() {
		return s.getPrimaryNode().getStats(STATS.IO_PAGE_READ_CNT_UNQ);
	}

	public int getStorageDataPageReadCount() {
		return s.getPrimaryNode().getStats(STATS.IO_DATA_PAGE_READ_CNT);
	}

	public int getStorageDataPageReadCountUnique() {
		return s.getPrimaryNode().getStats(STATS.IO_DATA_PAGE_READ_CNT_UNQ);
	}

	public int getStat(STATS stat) {
		return s.getPrimaryNode().getStats(stat);
	}
}
