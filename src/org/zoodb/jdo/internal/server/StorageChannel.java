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

import java.nio.ByteBuffer;


/**
 * A StorageChannel manages a database file and provides read- and 
 * write-views on it. 
 * 
 * @author Tilmann Zäschke
 *
 */
public interface StorageChannel {

	StorageChannelInput getReader(boolean autoPaging);

	StorageChannelOutput getWriter(boolean autoPaging);

	void reportFreePage(int pageId);

	int getPageSize();

	void close();

	void flush();

	int statsGetReadCount();

	int statsGetWriteCount();

	void write(ByteBuffer buf, long currentPage);

	void readPage(ByteBuffer buf, long pageId);

}
