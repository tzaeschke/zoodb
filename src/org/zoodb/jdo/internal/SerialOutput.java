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
package org.zoodb.jdo.internal;


/**
 * This class is similar to DataOutput. For simplicity and for performance reasons, the 
 * SerialOutput class has been created.
 * 
 * There will be two implementations. One for writing directly to disk, and one for writing to a
 * network socket.
 * 
 * @author Tilmann Zäschke
 *
 */
public interface SerialOutput {

	void writeInt(int size);

	void writeBoolean(boolean boolean1);

	void writeByte(byte byte1);

	void writeChar(char char1);

	void writeFloat(float float1);

	void writeDouble(double double1);

	void writeLong(long long1);

	void writeShort(short short1);

	void write(byte[] array);

    void writeString(String string);

	void skipWrite(int nBytes);

}
