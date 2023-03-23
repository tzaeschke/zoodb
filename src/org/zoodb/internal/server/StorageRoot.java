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

import java.nio.ByteBuffer;

public interface StorageRoot {

	void close(IOResourceProvider channel);

	void force();

	int statsGetPageCount();

	int statsGetReadCount();

	int statsGetReadCountUnique();

	int statsGetWriteCount();

	void readPage(ByteBuffer buf, long pageId);

	void write(ByteBuffer buf, long pageId);

	int getPageSize();

	void reportFreePage(int pageId);

	int getNextPage(int prevPage);

	IOResourceProvider createChannel();

	int getDataChannelCount();

	IOResourceProvider getIndexChannel();

	void close();

	boolean closeIfNoChannelsRemain();

	boolean debugIsPageIdInFreeList(int pageId);
	
}
