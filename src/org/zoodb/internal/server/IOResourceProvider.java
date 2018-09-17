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
