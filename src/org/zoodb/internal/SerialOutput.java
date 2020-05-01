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
 * This class is similar to DataOutput. For simplicity and for performance reasons, the 
 * SerialOutput class has been created.
 * 
 * There will be two implementations. One for writing directly to disk, and one for writing to a
 * network socket.
 * 
 * @author Tilmann Zaeschke
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
