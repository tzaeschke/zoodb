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
package org.zoodb.internal.server.index;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.StorageChannel;

public class IndexFactory {

	/**
	 * @param type
	 * @param storage
	 * @return a new index
	 */
	public static LongLongIndex createIndex(PAGE_TYPE type, StorageChannel storage) {
		return new PagedLongLong(type, storage);
	}
	
	/**
	 * @param type
	 * @param storage
	 * @param pageId page id of the root page
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex loadIndex(PAGE_TYPE type, StorageChannel storage, int pageId) {
		return new PagedLongLong(type, storage, pageId);
	}
	
	/**
	 * @param type
	 * @param storage
	 * @return a new index
	 */
	public static LongLongIndex.LongLongUIndex createUniqueIndex(PAGE_TYPE type, 
			StorageChannel storage) {
		return new PagedUniqueLongLong(type, storage);
	}
	
	/**
	 * @param type
	 * @param storage
	 * @param pageId page id of the root page
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex.LongLongUIndex loadUniqueIndex(PAGE_TYPE type, 
			StorageChannel storage, int pageId) {
		return new PagedUniqueLongLong(type, storage, pageId);
	}
	
	/**
	 * EXPERIMENTAL! Index that has bit width of key and value as parameters.
	 * @param type
	 * @param storage
	 * @return a new index
	 */
	public static LongLongIndex.LongLongUIndex createUniqueIndex(PAGE_TYPE type, 
			StorageChannel storage, int keySize, int valSize) {
		return new PagedUniqueLongLong(type, storage, keySize, valSize);
	}
	
	/**
	 * EXPERIMENTAL! Index that has bit width of key and value as parameters.
	 * @param type
	 * @param storage
	 * @param pageId page id of the root page
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex.LongLongUIndex loadUniqueIndex(PAGE_TYPE type, 
			StorageChannel storage, int pageId, int keySize, int valSize) {
		return new PagedUniqueLongLong(type, storage, pageId, keySize, valSize);
	}
	
}
