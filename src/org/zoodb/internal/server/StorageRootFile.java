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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;

import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.PrimLongSetZ;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.ZooDebug;

/**
 * A common root for multiple file views. Each view accesses its own page,
 * the root contains the common file resource.
 * 
 * @author Tilmann Zaeschke
 *
 */
public final class StorageRootFile implements StorageRoot {

	private final ArrayList<IOResourceProvider> views = new ArrayList<>();
	private final StorageChannelImpl indexChannel;

	private final FreeSpaceManager fsm;
	private final RandomAccessFile raf;
	private final FileLock fileLock;
	private final FileChannel fc;
	// use LONG to enforce long-arithmetic in calculations
	private final long PAGE_SIZE;

	private int statNRead; 
	private int statNWrite; 
	private final PrimLongSetZ statNReadUnique = new PrimLongSetZ();

	public StorageRootFile(String dbPath, String options, int pageSize, FreeSpaceManager fsm,
			DiskAccess session) {
		this.fsm = fsm;
		PAGE_SIZE = pageSize;
		File file = new File(dbPath);
		if (!file.exists()) {
			throw DBLogger.newUser("DB file does not exist: " + dbPath);
		}
		try {
			raf = new RandomAccessFile(file, options);
			fc = raf.getChannel();
			try {
				//tryLock is supposed to return null, but it throws an Exception
				fileLock = fc.tryLock();
				if (fileLock == null) {
					fc.close();
					raf.close();
					throw DBLogger.newUser("This file is in use by another process: " + dbPath);
				}
			} catch (OverlappingFileLockException e) {
				fc.close();
				raf.close();
				throw DBLogger.newUser(
						"This file is in use by another PersistenceManager: " + dbPath);
			}
			if (ZooDebug.isTesting()) {
				ZooDebug.registerFile(fc);
			}
		} catch (IOException e) {
			throw DBLogger.newFatal("Error opening database: " + dbPath, e);
		}
		this.indexChannel = new StorageChannelImpl(this, session);
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
	public final void close() {
		indexChannel.close();
		//TODO flush();
		try {
			fc.force(true);
			fileLock.release();
			fc.close();
			raf.close();
		} catch (IOException e) {
			throw DBLogger.newFatal("Error closing database file.", e);
		}
	}

	@Override
	public void close(StorageChannel channel) {
		if (!views.remove(channel) && channel != indexChannel) {
			throw new IllegalStateException();
		}
	}

	@Override
	public void force() {
		indexChannel.flushNoForce();
		try {
			fc.force(false);
		} catch (IOException e) {
			throw DBLogger.newFatal("Error writing database file.", e);
		}
	}

	@Override
	public final IOResourceProvider createChannel(DiskAccess session) {
		IOResourceProvider c = new StorageChannelImpl(this, session);
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
	public final void readPage(ByteBuffer buf, long pageId) {
		try {
			fc.read(buf, pageId * PAGE_SIZE);
			if (DBStatistics.isEnabled()) {
				statNRead++;
				statNReadUnique.add(pageId);
			}
		} catch (IOException e) {
			throw DBLogger.newFatal("Error loading Page: " + pageId, e);
		}
	}

	@Override
	public final void write(ByteBuffer buf, long pageId) {
		try {
			if (pageId<0) {
				return;
			}
			if (DBStatistics.isEnabled()) {
				statNWrite++;
			}
			fc.write(buf, pageId * PAGE_SIZE);
		} catch (IOException e) {
			throw DBLogger.newFatal("Error writing page: " + pageId, e);
		}
	}

	@Override
	public final int statsGetReadCount() {
		return statNRead;
	}

	@Override
	public int statsGetReadCountUnique() {
		int ret = statNReadUnique.size();
		statNReadUnique.clear();
		return ret;
	}

	@Override
	public final int statsGetWriteCount() {
		return statNWrite;
	}

	@Override
	public final int getPageSize() {
		return (int) PAGE_SIZE;
	}

	@Override
	public int statsGetPageCount() {
		try {
			return (int) (raf.length() / PAGE_SIZE);
		} catch (IOException e) {
			throw DBLogger.newFatal("", e);
		}
	}

	@Override
	public boolean debugIsPageIdInFreeList(int pageId) {
		return fsm.debugIsPageIdInFreeList(pageId);
	}

}
