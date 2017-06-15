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

public interface StorageRoot {

	//TODO close
	public void close(StorageChannel channel);

	public void force();

	public int statsGetPageCount();

	public int statsGetReadCount();

	public int statsGetReadCountUnique();

	public int statsGetWriteCount();

	public void readPage(ByteBuffer buf, long pageId);

	public void write(ByteBuffer buf, long pageId);

	int getPageSize();

	void reportFreePage(int pageId);

	public int getNextPage(int prevPage);

	StorageChannel createChannel();

	public void newTransaction(long txId);

	public int getDataChannelCount();

	public StorageChannel getIndexChannel();

	public void close();
	
}
