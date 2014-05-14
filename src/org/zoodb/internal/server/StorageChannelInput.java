/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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

import org.zoodb.internal.SerialInput;

/**
 * 
 * @author Tilmann Zaeschke
 *
 */
public interface StorageChannelInput extends SerialInput, DiskIO {

	void seekPageForRead(DATA_TYPE type, int nextPage);
	/**
	 * Assumes autopaging=true.
	 * @param pos
	 */
	public void seekPosAP(DATA_TYPE type, long pos);

	public void seekPage(DATA_TYPE type, int page, int offs);

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
