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

}
