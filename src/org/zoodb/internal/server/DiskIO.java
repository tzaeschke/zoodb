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
		PAGE_TYPE(int id) {
			this(id, true);
		}
		PAGE_TYPE(int id, boolean hasHeader) {
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
