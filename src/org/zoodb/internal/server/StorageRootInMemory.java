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

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.util.PrimLongSetZ;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.impl.DataStoreManagerInMemory;

public class StorageRootInMemory implements StorageRoot {

	private final ArrayList<IOResourceProvider> views = new ArrayList<>();
	private final StorageChannelImpl indexChannel;

	private final FreeSpaceManager fsm;
	// use bucket version of array List
	private final ArrayList<ByteBuffer> buffers;

	private final int PAGE_SIZE;
	
	private int statNRead = 0;
	private int statNWrite = 0;
	private final PrimLongSetZ statNReadUnique = new PrimLongSetZ();
	
	/**
	 * Constructor for use by DataStoreManager.
	 * @param dbPath The database file path
	 * @param options I/O options (not used here)
	 * @param pageSize The default page size in bytes
	 * @param fsm The free space manager instance
	 */
	public StorageRootInMemory(String dbPath, String options, int pageSize, 
			FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		this.fsm = fsm;
		// We keep the arguments to allow transparent dependency injection.
		buffers = DataStoreManagerInMemory.getInternalData(dbPath);
		this.indexChannel = new StorageChannelImpl(this);
	}
	
	/**
	 * Special constructor for testing only.
	 * @param pageSize page size
	 */
	public StorageRootInMemory(int pageSize) {
		this(pageSize, new FreeSpaceManager());
	}
	
	/**
	 * Special constructor for testing only.
	 * @param pageSize page size
	 * @param fsm FreeSpaceManager
	 */
	public StorageRootInMemory(int pageSize, FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		this.fsm = fsm;
		this.indexChannel = new StorageChannelImpl(this);
    	fsm.initBackingIndexNew(indexChannel);
    	fsm.getNextPage(0);  // avoid using first page
		// We keep the arguments to allow transparent dependency injection.
		buffers = new ArrayList<>();
	}

	@Override
	public int getNextPage(int prevPage) {
		return fsm.getNextPage(prevPage);
	}

	@Override
	public void reportFreePage(int pageId) {
		fsm.reportFreePage(pageId);
	}

	@Override
	public void close() {
		indexChannel.close();
	}

	@Override
	public void close(StorageChannel channel) {
		if (!views.remove(channel) && channel != indexChannel) {
			throw new IllegalStateException();
		}
//		if (views.isEmpty()) {
//			System.err.println("Don't forget to close the file!");
//			//TODO close everything???
//		}
	}

	@Override
	public void force() {
		indexChannel.flushNoForce();
		//Nothing else, we can't flush to memory... 
	}

	@Override
	public final IOResourceProvider createChannel() {
		IOResourceProvider c = new StorageChannelImpl(this);
		views.add(c);
		return c;
	}

	@Override
	public final IOResourceProvider getIndexChannel() {
		return indexChannel;
	}

	@Override
	public int getDataChannelCount() {
		return views.size();
	}

	@Override
	public void readPage(ByteBuffer buf, long pageId) {
		ByteBuffer b2 = buffers.get((int) pageId);
		b2.rewind();
		buf.put(b2);
		if (DBStatistics.isEnabled()) {
			statNRead++;
			statNReadUnique.add(pageId);
		}
	}

	@Override
	public void write(ByteBuffer buf, long pageId) {
		if (pageId<0) {
			return;
		}
		if (DBStatistics.isEnabled()) {
			statNWrite++;
		}
		while (pageId >= buffers.size()) {
			buffers.add(ByteBuffer.allocateDirect(PAGE_SIZE));
		}
		ByteBuffer b2 = buffers.get((int) pageId);
		b2.clear();
		b2.put(buf);
	}
	
	@Override
	public int statsGetReadCount() {
		return statNRead;
	}

	@Override
	public int statsGetReadCountUnique() {
		int ret = statNReadUnique.size();
		statNReadUnique.clear();
		return ret;
	}

	@Override
	public int statsGetWriteCount() {
		return statNWrite;
	}

	@Override
	public int getPageSize() {
		return PAGE_SIZE;
	}

	@Override
	public int statsGetPageCount() {
		return buffers.size();
	}
}
