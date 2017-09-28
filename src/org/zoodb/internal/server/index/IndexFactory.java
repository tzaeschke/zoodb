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
package org.zoodb.internal.server.index;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;

public class IndexFactory {

	public static interface CreateIndex<R> {
		R create(PAGE_TYPE type, IOResourceProvider storage);
	}
	
	public static interface LoadIndex<R> {
		R load(PAGE_TYPE type, IOResourceProvider storage, int pageId);
	}
	
	public static interface CreateIndexSized<R> {
		R create(PAGE_TYPE type, IOResourceProvider storage, int keySize, int valSize);
	}
	
	public static interface LoadIndexSized<R> {
		R load(PAGE_TYPE type, IOResourceProvider storage, int pageId, int keySize, int valSize);
	}
	
	private static final CreateIndex<LongLongIndex> DEFAULT_CREATE_INDEX = PagedLongLong::new;
	private static final LoadIndex<LongLongIndex> DEFAULT_LOAD_INDEX = PagedLongLong::new;
	private static final CreateIndex<LongLongIndex.LongLongUIndex> DEFAULT_CREATE_UNIQUE_INDEX = 
			PagedUniqueLongLong::new;
	private static final LoadIndex<LongLongIndex.LongLongUIndex> DEFAULT_LOAD_UNIQUE_INDEX = 
			PagedUniqueLongLong::new;
	private static final CreateIndexSized<LongLongIndex.LongLongUIndex> DEFAULT_CREATE_UNIQUE_INDEX_SIZED = 
			PagedUniqueLongLong::new;
	private static final LoadIndexSized<LongLongIndex.LongLongUIndex> DEFAULT_LOAD_UNIQUE_INDEX_SIZED = 
			PagedUniqueLongLong::new;
					
	public static volatile CreateIndex<LongLongIndex> CREATE_INDEX = DEFAULT_CREATE_INDEX;
	public static volatile CreateIndex<LongLongIndex.LongLongUIndex> CREATE_UNIQUE_INDEX = DEFAULT_CREATE_UNIQUE_INDEX;
	public static volatile LoadIndex<LongLongIndex> LOAD_INDEX = DEFAULT_LOAD_INDEX;
	public static volatile LoadIndex<LongLongIndex.LongLongUIndex> LOAD_UNIQUE_INDEX = DEFAULT_LOAD_UNIQUE_INDEX;
	public static volatile CreateIndexSized<LongLongIndex.LongLongUIndex> CREATE_UNIQUE_INDEX_SIZED = 
			DEFAULT_CREATE_UNIQUE_INDEX_SIZED;
	public static volatile LoadIndexSized<LongLongIndex.LongLongUIndex> LOAD_UNIQUE_INDEX_SIZED = 
			DEFAULT_LOAD_UNIQUE_INDEX_SIZED;
	
	private IndexFactory() {
		//private
	}
	
	/**
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @return a new index
	 */
	public static LongLongIndex createIndex(PAGE_TYPE type, IOResourceProvider storage) {
		return CREATE_INDEX.create(type, storage);
	}
	
	/**
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @param pageId page id of the root page
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex loadIndex(PAGE_TYPE type, IOResourceProvider storage, int pageId) {
		return LOAD_INDEX.load(type, storage, pageId);
	}
	
	/**
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @return a new index
	 */
	public static LongLongIndex.LongLongUIndex createUniqueIndex(PAGE_TYPE type, 
			IOResourceProvider storage) {
		return CREATE_UNIQUE_INDEX.create(type, storage);
	}
	
	/**
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @param pageId page id of the root page
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex.LongLongUIndex loadUniqueIndex(PAGE_TYPE type, 
			IOResourceProvider storage, int pageId) {
		return LOAD_UNIQUE_INDEX.load(type, storage, pageId);
	}
	
	/**
	 * EXPERIMENTAL! Index that has bit width of key and value as parameters.
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @param keySize The number of bytes required by the key
	 * @param valSize The number of bytes required by the value
	 * @return a new index
	 */
	public static LongLongIndex.LongLongUIndex createUniqueIndex(PAGE_TYPE type, 
			IOResourceProvider storage, int keySize, int valSize) {
		return CREATE_UNIQUE_INDEX_SIZED.create(type, storage, keySize, valSize);
	}
	
	/**
	 * EXPERIMENTAL! Index that has bit width of key and value as parameters.
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @param pageId page id of the root page
	 * @param keySize The number of bytes required by the key
	 * @param valSize The number of bytes required by the value
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex.LongLongUIndex loadUniqueIndex(PAGE_TYPE type, 
			IOResourceProvider storage, int pageId, int keySize, int valSize) {
		return LOAD_UNIQUE_INDEX_SIZED.load(type, storage, pageId, keySize, valSize);
	}
	
}
