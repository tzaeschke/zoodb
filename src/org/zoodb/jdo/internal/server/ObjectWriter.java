/*
 * Copyright 2009-2013 Tilmann Zäschke. All rights reserved.
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

/**
 * This class serves as a mediator between the serializer and the file access class.
 * Compared to the StorageWrite, the ObjectWriter also provides the following:
 * - Updating the oid- and class-index with new object positions.
 * - Insert the page header (currently containing only the class-oid).
 * 
 * @author Tilmann Zaschke
 */
public interface ObjectWriter extends SerialOutput {

	public void startObject(long oid, int prevSchemaVersion);

	public void finishObject();

	public void flush();
	
	/**
	 * This can be necessary when subsequent objects are of a different class.
	 */
	public void newPage();
	
}
