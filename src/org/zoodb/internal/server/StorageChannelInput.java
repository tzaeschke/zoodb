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

import org.zoodb.internal.SerialInput;

/**
 * 
 * @author Tilmann Zaeschke
 *
 */
public interface StorageChannelInput extends SerialInput, DiskIO {

	void seekPageForRead(PAGE_TYPE type, int nextPage);
	/**
	 * Assumes autopaging=true.
	 * @param type page type
	 * @param pos position
	 */
	void seekPosAP(PAGE_TYPE type, long pos);

	void seekPage(PAGE_TYPE type, int page, int offs);

	int getOffset();

    int getPage();  

	void noCheckRead(long[] array);

	void noCheckRead(int[] array);

	void noCheckRead(byte[] array);

	void noCheckRead(long[] array, int len);

	void noCheckRead(int[] array, int len);

	void noCheckRead(byte[] array, int len);

	void noCheckReadAsInt(long[] array, int nElements);

	void reset();

	void setOverflowCallbackRead(CallbackPageRead readCallback);

}
