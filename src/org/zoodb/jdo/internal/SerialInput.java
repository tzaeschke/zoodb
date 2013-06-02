/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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
 * This class is similar to DataInput. For simplicity and for performance reasons, the 
 * SerialInput class has been created.
 * 
 * There will be two implementations. One for reading directly from disk, and one for reading from a
 * network socket.
 * 
 * @author Tilmann Zaeschke
 *
 */
public interface SerialInput {

	public int readInt();

	public long readLong();

	public boolean readBoolean();

	public byte readByte();

	public char readChar();

	public double readDouble();

	public float readFloat();

	public short readShort();

	public void readFully(byte[] array);

    public String readString();

	public void skipRead(int nBytes);

//	/**
//	 * Assumes autopaging=true.
//	 * @param pos
//	 */
//	public void seekPosAP(DATA_TYPE type, long pos);
//
//	public void seekPage(DATA_TYPE type, int page, int offs);

    public long getHeaderClassOID();

}
