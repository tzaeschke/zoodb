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
		IO_PAGE_READ_CNT(true),
		/** Page read access counter. Counts only unique access (each page counted only once). */
		IO_PAGE_READ_CNT_UNQ(true),
		/** Page write access counter. */
		IO_PAGE_WRITE_CNT(true),
		/** Data page (only stored objects) read access counter. */
		IO_DATA_PAGE_READ_CNT(true),
		/** Data page (only stored objects) read access counter. 
		 * Counts only unique access (each page counted only once). */
		IO_DATA_PAGE_READ_CNT_UNQ(true), 
		
		/** Number of pages used by free space manager. */
		DB_PAGE_CNT_IDX_FSM(true), 
		/** Number of pages used by OID index. */
		DB_PAGE_CNT_IDX_OID(true), 
		/** Number of pages used by POS index. */
		DB_PAGE_CNT_IDX_POS(true),
		/** Number of pages used by attribute indices. */
		DB_PAGE_CNT_IDX_ATTRIBUTES(true), 
		/** Number of pages used by data (serialized objects). */
		DB_PAGE_CNT_DATA(true),
		/** Total number of pages. */
		DB_PAGE_CNT(true), 
		
		/** Number of objects in buffered past transactions. */
		TX_MGR_BUFFERED_OID_CNT(true), 
		/** Number of buffered past transactions. */
		TX_MGR_BUFFERED_TX_CNT(true),
		
		/** Number of queries compiled. */
		QU_COMPILED(false),
		/** Number of queries executed. */
		QU_EXECUTED_TOTAL(false),
		/** Number of queries executed without index (using Extent) */
		QU_EXECUTED_WITHOUT_INDEX(false),
		/** Number of queries with ordering without index. */
		QU_EXECUTED_WITH_ORDERING_WITHOUT_INDEX(false);
		
		private final boolean isServerStat;
		private STATS(boolean isServerStat) {
			this.isServerStat = isServerStat;
		}
		boolean isServerStat() {
			return isServerStat;
		}
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
	public long getStoragePageReadCount() {
		return s.getPrimaryNode().getStats(STATS.IO_PAGE_READ_CNT);
	}

	/**
	 * 
	 * @return Number of written pages since the session was created. This includes pages that 
	 * are not written yet (commit pending) and pages that have been rolled back.
	 */
	public long getStoragePageWriteCount() {
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

	public long getStoragePageReadCountUnique() {
		return s.getPrimaryNode().getStats(STATS.IO_PAGE_READ_CNT_UNQ);
	}

	public long getStorageDataPageReadCount() {
		return s.getPrimaryNode().getStats(STATS.IO_DATA_PAGE_READ_CNT);
	}

	public long getStorageDataPageReadCountUnique() {
		return s.getPrimaryNode().getStats(STATS.IO_DATA_PAGE_READ_CNT_UNQ);
	}

	public long getQueryCompileCount() {
		return s.getStats(STATS.QU_COMPILED);
	}

	public long getQueryExecutionCount() {
		return s.getStats(STATS.QU_EXECUTED_TOTAL);
	}

	public long getQueryExecutionWithoutIndexCount() {
		return s.getStats(STATS.QU_EXECUTED_WITHOUT_INDEX);
	}

	public long getQueryExecutionWithOrderingWithoutIndexCount() {
		return s.getStats(STATS.QU_EXECUTED_WITH_ORDERING_WITHOUT_INDEX);
	}

	public long getStat(STATS stat) {
		if (stat.isServerStat()) {
			return s.getPrimaryNode().getStats(stat);
		} else {
			return s.getStats(stat);
		}
	}
}
