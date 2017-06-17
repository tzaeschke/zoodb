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

/**
 * A common root for multiple file views. Each view accesses its own page,
 * the root contains the common file resource.
 * 
 * @author Tilmann Zaeschke
 *
 */
public final class StorageChannelImpl implements StorageChannel {

	private final ArrayList<StorageChannelInput> viewsIn = new ArrayList<>();
	private final ArrayList<StorageChannelOutput> viewsOut = new ArrayList<>();
	
	private final StorageRoot root;
	private long txId;

	public StorageChannelImpl(StorageRoot root) {
		this.root = root;
	}

	@Override
	public void newTransaction(long txId) {
		this.txId = txId;
	}
	
	@Override
	public long getTxId() {
		return this.txId;
	}
	
	@Override
	public final void close() {
		flush();
		root.close(this);
	}

	@Override
	public final StorageChannelInput getReader(boolean autoPaging) {
		StorageChannelInput in = new StorageReader(this, autoPaging);
		viewsIn.add(in);
		return in;
	}
	
	@Override
	public final StorageChannelOutput getWriter(boolean autoPaging) {
		StorageChannelOutput out = new StorageWriter(this, autoPaging);
		viewsOut.add(out);
		return out;
	}
	
	/**
	 * Not a true flush, just writes the stuff...
	 */
	@Override
	public final void flush() {
		flushNoForce();
		root.force();
	}

	public void flushNoForce() {
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
	public int getNextPage(int prevPage) {
		return root.getNextPage(prevPage);
	}

	@Override
	public final void readPage(ByteBuffer buf, long pageId) {
		root.readPage(buf, pageId);
	}

	@Override
	public final void write(ByteBuffer buf, long pageId) {
		root.write(buf, pageId);
	}

	@Override
	@Deprecated //use root.xyz() 
	public final int statsGetReadCount() {
		return root.statsGetReadCount();
	}

	@Override
	@Deprecated //use root.xyz() 
	public int statsGetReadCountUnique() {
		return root.statsGetReadCountUnique();
	}

	@Override
	@Deprecated //use root.xyz() 
	public final int statsGetWriteCount() {
		return root.statsGetWriteCount();
	}

	@Override
	@Deprecated //use root.xyz() 
	public final int getPageSize() {
		return root.getPageSize();
	}

	@Override
	public void reportFreePage(int pageId) {
		root.reportFreePage(pageId);
	}

	@Override
	@Deprecated //use root.xyz() 
	public int statsGetPageCount() {
		return root.statsGetPageCount();
	}

}
