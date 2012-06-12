/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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

import org.zoodb.jdo.internal.SerialOutput;

public interface StorageChannelOutput extends SerialOutput {

	void seekPageForWrite(int nextPage);

	int getOffset();

    int getPage();  

	/**
	 * Allocate a new page. Auto-paging is disabled.
	 * @param autoPaging Whether auto paging should be used.
	 * @param previousPageId ID of the previous page or 0 if N/A. This will return the previous page
	 * to the free space manager.
	 * @return ID of the new page.
	 */
	int allocateAndSeek(int previousPageId);

	/**
	 * Allocate a new page. Auto-paging is enabled.
	 * @param previousPageId ID of the previous page or 0 if N/A. This will return the previous page
	 * to the free space manager.
	 * @param header The header to be used for that page.
	 */
	int allocateAndSeekAP(int previousPageId, long header);

	void noCheckWrite(long[] array);

	void noCheckWrite(int[] array);

	void noCheckWrite(byte[] array);

	/**
	 * Callback for page overflow (automatic allocation of following page).
	 * @param overflowCallback
	 */
	void setOverflowCallback(ObjectWriter overflowCallback);

	void noCheckWriteAsInt(long[] array, int nElements);

	/**
	 * Not a true flush, just writes the stuff to StorageChannel.
	 */
	void flush();

}
