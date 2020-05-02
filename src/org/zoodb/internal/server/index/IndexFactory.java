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
package org.zoodb.internal.server.index;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;

public class IndexFactory {

	public interface CreateIndex<R> {
		R create(PAGE_TYPE type, IOResourceProvider storage);
	}
	
	public interface LoadIndex<R> {
		R load(PAGE_TYPE type, IOResourceProvider storage, int pageId);
	}
	
	public interface CreateIndexSized<R> {
		R create(PAGE_TYPE type, IOResourceProvider storage, int keySize, int valSize);
	}
	
	public interface LoadIndexSized<R> {
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
