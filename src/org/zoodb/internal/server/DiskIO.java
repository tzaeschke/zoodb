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
package org.zoodb.internal.server;

public interface DiskIO {

	public static final int DB_FILE_TYPE_ID = 13031975;
	public static final int DB_FILE_VERSION_MAJ = 1;
	public static final int DB_FILE_VERSION_MIN = 5;

	static final short PAGE_FORMAT_VERSION = 1;
	
	public enum PAGE_TYPE {
		DB_HEADER(1, false), //not used
		ROOT_PAGE(2), //not used yet
		USERS(3), //not used yet
		OID_INDEX(4),
		FREE_INDEX(5),
		SCHEMA_INDEX(6),
		//SCHEMA_INDEX_OVERFLOW(7),
		INDEX_CATALOG(8),
		//INDEX_MGR_OVERFLOW(9),
		DATA(10),
		//DATA_OVERFLOW(11),
		GENERIC_INDEX(12),
		POS_INDEX(13),
		FIELD_INDEX(14),
		;
		
		private final byte id;
		private final boolean hasHeader;
		private PAGE_TYPE(int id) {
			this(id, true);
		}
		private PAGE_TYPE(int id, boolean hasHeader) {
			this.id = (byte) id;
			this.hasHeader = hasHeader;
			
		}
		public byte getId() {
			return id;
		}
		public boolean hasHeader() {
			return hasHeader;
		}
	}
	
	//private static final int S_BOOL = 1;
	static final int S_BYTE = 1;
	static final int S_CHAR = 2;
	static final int S_DOUBLE = 8;
	static final int S_FLOAT = 4;
	static final int S_INT = 4;
	static final int S_LONG = 8;
	static final int S_SHORT = 2;
	
	static final int PAGE_HEADER_SIZE = 12; //type, dummy, tx-id
	static final int PAGE_HEADER_SIZE_DATA = PAGE_HEADER_SIZE + 8; //class-oid

}
