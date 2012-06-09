/*
 * Copyright 2009-2012 Tilmann Zäschke. All rights reserved.
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;

import javax.jdo.JDOFatalDataStoreException;
import javax.jdo.JDOFatalUserException;
import javax.jdo.JDOUserException;

import org.zoodb.jdo.internal.server.index.FreeSpaceManager;

/**
 * A common root for multiple file views. Each view accesses its own page,
 * the root contains the common file resource.
 * 
 * @author Tilmann Zäschke
 *
 */
public final class PageAccessFile_BBRoot implements StorageChannel {

	private final ArrayList<StorageChannelInput> viewsIn = new ArrayList<StorageChannelInput>();
	private final ArrayList<StorageChannelOutput> viewsOut = new ArrayList<StorageChannelOutput>();

	private final FreeSpaceManager fsm;
	private final RandomAccessFile raf;
	private final FileLock fileLock;
	private final FileChannel fc;
	// use LONG to enforce long-arithmetic in calculations
	private final long PAGE_SIZE;

	private int statNWrite; 

	public PageAccessFile_BBRoot(String dbPath, String options, int pageSize, FreeSpaceManager fsm) {
		this.fsm = fsm;
		PAGE_SIZE = pageSize;
		File file = new File(dbPath);
		if (!file.exists()) {
			throw new JDOUserException("DB file does not exist: " + dbPath);
		}
		try {
			raf = new RandomAccessFile(file, options);
			fc = raf.getChannel();
			try {
				//tryLock is supposed to return null, but it throws an Exception
				fileLock = fc.tryLock();
			} catch (OverlappingFileLockException e) {
				fc.close();
				raf.close();
				throw new JDOFatalUserException(
						"The file is already accessed by another process: " + dbPath);
			}
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error opening database: " + dbPath, e);
		}
	}

	@Override
	public final void close() {
		flush();
		try {
			fc.force(true);
			fileLock.release();
			fc.close();
			raf.close();
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error closing database file.", e);
		}
	}

	@Override
	public final StorageChannelInput getReader() {
		StorageChannelInput in = new StorageReader(this);
		viewsIn.add(in);
		return in;
	}
	
	@Override
	public final StorageChannelOutput getOutput() {
		StorageChannelOutput out = new StorageWriter(this, fsm);
		viewsOut.add(out);
		return out;
	}
	
	final FileChannel getFileChannel() {
		return fc;
	}

	/**
	 * Not a true flush, just writes the stuff...
	 */
	@Override
	public final void flush() {
		//flush associated splits.
		for (StorageChannelOutput paf: viewsOut) {
			//TODO flush() only writers
			paf.flush();
		}
		for (StorageChannelInput paf: viewsIn) {
			paf.reset();
		}
		try {
			fc.force(false);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error writing database file.", e);
		}
	}

	@Override
	public final void readPage(ByteBuffer buf, long pageId) {
		try {
			fc.read(buf, pageId * PAGE_SIZE);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error loading Page: " + pageId, e);
		}
	}

	@Override
	public final void write(ByteBuffer buf, long pageId) {
		try {
			if (pageId<0) {
				return;
			}
			statNWrite++;
			fc.write(buf, pageId * PAGE_SIZE);
		} catch (IOException e) {
			throw new JDOFatalDataStoreException("Error writing page: " + pageId, e);
		}
	}

	@Override
	public final int statsGetWriteCount() {
		return statNWrite;
	}

	public final int getPageSize() {
		return (int) PAGE_SIZE;
	}

	@Override
	public void releasePage(int pageId) {
		fsm.reportFreePage(pageId);
	}

}
