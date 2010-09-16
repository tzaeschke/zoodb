package org.zoodb.jdo.internal;


/**
 * This class is similar to DataInput. For simplicity and for performance reasons, the 
 * SerialInput class has been created.
 * 
 * There will be two implementations. One for reading directly from disk, and one for reading from a
 * network socket.
 * 
 * @author Tilmann Zäschke
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

}
