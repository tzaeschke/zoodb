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
package org.zoodb.jdo.internal.server;

import static org.junit.Assert.*;

import org.junit.Test;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.tools.ZooConfig;

/**
 * 
 * @author Tilmann Zäschke
 * @author Jonas Nick
 */
public class TestStorageChannel {

	@Test
	public void testDelete() {
		FreeSpaceManager fsm = new FreeSpaceManager();
		StorageRootInMemory storage = new StorageRootInMemory(ZooConfig.getFilePageSize(), fsm);
		StorageChannelOutput out = storage.createChannel().createWriter(false);
		assertEquals(0, storage.statsGetPageCount());
		int pageId = out.allocateAndSeek(PAGE_TYPE.GENERIC_INDEX, 0);
		assertEquals(0, storage.statsGetPageCount());
		out.writeShort((short) -1);
		out.flush();
		assertEquals(2, storage.statsGetPageCount());
		System.out.println(fsm.debugPageIds());
		storage.reportFreePage(pageId);
		assertEquals(2, storage.statsGetPageCount());
		assertTrue(fsm.debugIsPageIdInFreeList(pageId));
	}

}
