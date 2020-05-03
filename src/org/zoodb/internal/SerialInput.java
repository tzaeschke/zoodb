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
package org.zoodb.internal;




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

	int readInt();

	long readLong();

	boolean readBoolean();

	byte readByte();

	char readChar();

	double readDouble();

	float readFloat();

	short readShort();

	void readFully(byte[] array);

    String readString();

	void skipRead(int nBytes);

    long getHeaderClassOID();

	long getHeaderTimestamp();

}
