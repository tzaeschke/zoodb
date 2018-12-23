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
package org.zoodb.jdo.internal.server;

import static org.junit.Assert.*;

import org.junit.Test;
import org.zoodb.internal.server.DiskAccess;
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
		StorageRootInMemory storage = new StorageRootInMemory(ZooConfig.getFilePageSize(), fsm, DiskAccess.NULL);
		StorageChannelOutput out = storage.createChannel(DiskAccess.NULL).createWriter(false, DiskAccess.NULL);
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
