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

import org.zoodb.internal.SerialOutput;

public interface StorageChannelOutput extends SerialOutput, DiskIO {

	void seekPageForWrite(PAGE_TYPE type, int nextPage);

	int getOffset();

    int getPage();  

	/**
	 * Allocate a new page. Auto-paging is disabled.
	 * @param previousPageId ID of the previous page or 0 if N/A. This will return the previous page
	 * to the free space manager.
	 * @return ID of the new page.
	 */
	int allocateAndSeek(PAGE_TYPE type, int previousPageId);

	/**
	 * Allocate a new page. Auto-paging is enabled.
	 * @param previousPageId ID of the previous page or 0 if N/A. This will return the previous page
	 * to the free space manager.
	 * @param header The header to be used for that page.
	 */
	int allocateAndSeekAP(PAGE_TYPE type, int previousPageId, long header);

	void noCheckWrite(long[] array);

	void noCheckWrite(int[] array);

	void noCheckWrite(byte[] array);

	void noCheckWrite(long[] array, int len);

	void noCheckWrite(int[] array, int len);

	void noCheckWrite(byte[] array, int len);

	/**
	 * Callback for page overflow (automatic allocation of following page).
	 * @param overflowCallback
	 */
	void setOverflowCallbackWrite(CallbackPageWrite overflowCallback);

	void noCheckWriteAsInt(long[] array, int nElements);

	/**
	 * Not a true flush, just writes the stuff to StorageChannel.
	 */
	void flush();

}
