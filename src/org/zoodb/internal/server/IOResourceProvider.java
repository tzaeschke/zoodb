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

import java.util.function.ToIntFunction;

public interface IOResourceProvider {

	void reportFreePage(int pageId);

	long getTxId();

	int getPageSize();

	/**
	 * This method is thread safe.
	 * @return a temporary input channel (with autopaging enabled)
	 */
	StorageChannelInput getInputChannel();

	void returnInputChannel(StorageChannelInput in);

	void flush();

	int writeIndex(ToIntFunction<StorageChannelOutput> writer);
	
	void startWriting(long txId);

	int statsGetReadCount();

	int statsGetReadCountUnique();

	int statsGetWriteCount();

	int statsGetPageCount();

	/**
	 * Create a managed input channel. This method is NOT thread safe.
	 * @param autoPaging Whether a new page should be allocated when the end of 
	 * the page is reached.
	 * @return a new input channel
	 */
	StorageChannelInput createReader(boolean autoPaging);
	
	/**
	 * Create a managed output channel. This method is NOT thread safe.
	 * @param autoPaging Whether a new page should be allocated when the end of 
	 * the page is reached.
	 * @return a new output channel
	 */
	StorageChannelOutput createWriter(boolean autoPaging);

	/**
	 * Drop an input channel. This method is slow and NOT thread safe.
	 * @param in the input channel
	 */
	void dropReader(StorageChannelInput in);
	
	void close();

	boolean debugIsPageIdInFreeList(int pageId);
}
