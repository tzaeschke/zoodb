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
package org.zoodb.jdo.internal.server;

public interface DiskIO {

	static final short PAGE_FORMAT_VERSION = 1;
	
//	static final byte PAGE_TYPE_FIRST = 1; //not used
//	static final byte PAGE_TYPE_ROOT_PAGE = 2; //not used yet
//	static final byte PAGE_TYPE_USERS = 3; //not used yet
//	static final byte PAGE_TYPE_OID_INDEX = 4;
//	static final byte PAGE_TYPE_FREE_INDEX = 5;
//	static final byte PAGE_TYPE_SCHEMA_INDEX = 6;
//	static final byte PAGE_TYPE_SCHEMA_INDEX_OVERFLOW = 7;
//	static final byte PAGE_TYPE_INDEX_MGR = 8;
//	static final byte PAGE_TYPE_INDEX_MGR_OVERFLOW = 9;
//	static final byte PAGE_TYPE_DATA_START = 10;
//	static final byte PAGE_TYPE_DATA_OVERFLOW = 11;
	
	public enum DATA_TYPE {
		DB_HEADER(1, false), //not used
		ROOT_PAGE(2), //not used yet
		USERS(3), //not used yet
		OID_INDEX(4),
		FREE_INDEX(5),
		SCHEMA_INDEX(6),
		//SCHEMA_INDEX_OVERFLOW(7),
		INDEX_MGR(8),
		//INDEX_MGR_OVERFLOW(9),
		DATA(10),
		//DATA_OVERFLOW(11),
		GENERIC_INDEX(12),
		POS_INDEX(13),
		FIELD_INDEX(14),
		;
		
		private final byte id;
		private final boolean hasHeader;
		private DATA_TYPE(int id) {
			this.id = (byte) id;
			this.hasHeader = true;
		}
		private DATA_TYPE(int id, boolean hasHeader) {
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
