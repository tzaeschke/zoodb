/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.zoodb.jdo.api.impl.DBStatistics;
import org.zoodb.jdo.api.impl.DataStoreManagerInMemory;
import org.zoodb.jdo.internal.server.index.FreeSpaceManager;
import org.zoodb.jdo.internal.util.PrimLongMapLI;

public class StorageRootInMemory implements StorageChannel {

	private final ArrayList<StorageChannelInput> viewsIn = new ArrayList<StorageChannelInput>();
	private final ArrayList<StorageChannelOutput> viewsOut = new ArrayList<StorageChannelOutput>();

	private final FreeSpaceManager fsm;
	// use bucket version of array List
	private final ArrayList<ByteBuffer> buffers;

	private final int PAGE_SIZE;
	
	private int statNRead = 0;
	private int statNWrite = 0;
	private final PrimLongMapLI<Object> statNReadUnique = new PrimLongMapLI<Object>();;
	private long txId;
	
	/**
	 * Constructor for use by DataStoreManager.
	 * @param dbPath
	 * @param options
	 */
	public StorageRootInMemory(String dbPath, String options, int pageSize, 
			FreeSpaceManager fsm) {
		PAGE_SIZE = pageSize;
		this.fsm = fsm;
		// We keep the arguments to allow transparent dependency injection.
		buffers = DataStoreManagerInMemory.getInternalData(dbPath);
	}
	
	/**
	 * SPecial constructor for testing only.
	 * @param pageSize
	 */
	public StorageRootInMemory(int pageSize) {
		PAGE_SIZE = pageSize;
		this.fsm = new FreeSpaceManager();
    	fsm.initBackingIndexNew(this);
    	fsm.getNextPage(0);  // avoid using first page
		// We keep the arguments to allow transparent dependency injection.
		buffers = new ArrayList<ByteBuffer>();
	}
	
	@Override
	public StorageChannelOutput getWriter(boolean autoPaging) {
		StorageChannelOutput out = new StorageWriter(this, fsm, autoPaging);
		viewsOut.add(out);
		return out;
	}
	
	@Override
	public StorageChannelInput getReader(boolean autoPaging) {
		StorageChannelInput in = new StorageReader(this, autoPaging);
		viewsIn.add(in);
		return in;
	}
	
	@Override
	public void reportFreePage(int pageId) {
		fsm.reportFreePage(pageId);
	}

	@Override
	public void acquireLock(long txId) {
		this.txId = txId;
	}
	
	@Override
	public long getTxId() {
		return this.txId;
	}
	
	@Override
	public void close() {
		flush();
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	@Override
	public void flush() {
		//flush associated splits.
		for (StorageChannelOutput paf: viewsOut) {
			//flush() only writers
			paf.flush();
		}
		for (StorageChannelInput paf: viewsIn) {
			paf.reset();
		}
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
	public void readPage(ByteBuffer buf, long pageId) {
		ByteBuffer b2 = buffers.get((int) pageId);
		b2.rewind();
		buf.put(b2);
		if (DBStatistics.isEnabled()) {
			statNRead++;
			statNReadUnique.put(pageId, null);
		}
	}

	@Override
	public int statsGetPageCount() {
		return buffers.size();
	}
}
