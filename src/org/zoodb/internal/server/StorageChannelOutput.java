/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal.server;

import org.zoodb.internal.SerialOutput;

public interface StorageChannelOutput extends SerialOutput, DiskIO {

	void seekPageForWrite(PAGE_TYPE type, int nextPage);

	int getOffset();

    int getPage();  

	/**
	 * Allocate a new page. Auto-paging is disabled.
	 * @param type page type
	 * @param previousPageId ID of the previous page or 0 if N/A. This will return the previous page
	 * to the free space manager.
	 * @return ID of the new page.
	 */
	int allocateAndSeek(PAGE_TYPE type, int previousPageId);

	/**
	 * Allocate a new page. Auto-paging is enabled.
	 * @param type page type
	 * @param previousPageId ID of the previous page or 0 if N/A. This will return the previous page
	 * to the free space manager.
	 * @param header The header to be used for that page.
	 * @return the page ID of the allocated page
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
	 * @param overflowCallback The overflow callback
	 */
	void setOverflowCallbackWrite(CallbackPageWrite overflowCallback);

	void noCheckWriteAsInt(long[] array, int nElements);

	/**
	 * Not a true flush, just writes the stuff to StorageChannel.
	 */
	void flush();

}
