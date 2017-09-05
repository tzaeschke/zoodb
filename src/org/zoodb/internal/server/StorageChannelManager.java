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

import java.util.ArrayList;

/**
 * 
 * @author Tilmann Zaeschke
 *
 */
public final class StorageChannelManager {

	private final ArrayList<StorageChannel> views = new ArrayList<>();
	private final StorageChannelImpl indexChannel;

	// use LONG to enforce long-arithmetic in calculations
	private final long PAGE_SIZE;
	private final StorageRoot root;

	public StorageChannelManager(StorageRoot root, int pageSize) {
		PAGE_SIZE = pageSize;
		this.root = root;
		this.indexChannel = new StorageChannelImpl(root);
	}

	public final void close() {
		indexChannel.close();
		root.close();
	}

	public void close(StorageChannel channel) {
		if (!views.remove(channel) && channel != indexChannel) {
			throw new IllegalStateException();
		}
	}

	public void force() {
		indexChannel.flushNoForce();
		root.force();
	}

	public final StorageChannel createChannel() {
		StorageChannel c = new StorageChannelImpl(root);
		views.add(c);
		return c;
	}

	public final StorageChannel getIndexChannel() {
		return indexChannel;
	}

	public int getDataChannelCount() {
		return views.size();
	}
	
	public final int getPageSize() {
		return (int) PAGE_SIZE;
	}

}
