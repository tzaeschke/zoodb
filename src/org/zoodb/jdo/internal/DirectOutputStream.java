package org.zoodb.jdo.internal;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * The class implements a buffered output stream that directly writes to a 
 * channel. No underlying OutStream code is called.
 * <p>
 * The code in this class is partially based on the the original SUN java code 
 * (Java 5) of the following classes:<br>
 * - java.io.BufferedOutPutStream<br> 
 * - java.io.DataOutputStream<br>
 * - java.nio.channels.Channels<br>
 * 
 * @author Tilmann Zaeschke
 */
public final class DirectOutputStream extends OutputStream 
implements DataOutput {

    /**
     * The internal buffer where data is stored. 
     */
    private byte _buf[];

    /**
     * The number of valid bytes in the buffer. This value is always 
     * in the range <tt>0</tt> through <tt>buf.length</tt>; elements 
     * <tt>buf[0]</tt> through <tt>buf[count-1]</tt> contain valid 
     * byte data.
     */
    private int _bufCount;
    private long _written = 0;

    private final WritableByteChannel _channel;
    private final ByteBuffer _bbb;
    private final byte[] _b4 = new byte[4];
    private final byte[] _b8 = new byte[8];

    /**
     * @param channel
     */
    public DirectOutputStream(WritableByteChannel channel) {
        this(channel, 1400);
    }
    /**
     * @param channel
     * @param size
     */
    public DirectOutputStream(WritableByteChannel channel, int size) {
        _channel = channel;
        _buf = new byte[size];
        _bbb = ByteBuffer.wrap(_buf);
    }

    public final void close() throws IOException {
        flushBuffer();
        _channel.close();
    }

    /** Flush the internal buffer */
    private final void flushBuffer() throws IOException {
        if (_bufCount > 0) {
            _bbb.limit(_bufCount);
            _bbb.position(0);
            _channel.write(_bbb);
            _bufCount = 0;
        }
    }

    /**
     * Writes the specified byte to this buffered output stream. 
     *
     * @param      b   the byte to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void write(int b) throws IOException {
        if (_bufCount >= _buf.length) {
            flushBuffer();
        }
        _buf[_bufCount++] = (byte)b;
        _written++;
    }

    /**
     * Writes the specified byte to this buffered output stream. 
     *
     * @param      b   the byte to be written.
     * @exception  IOException  if an I/O error occurs.
     */
//    public final void writeSingleByte(byte b) throws IOException {
//        if (_bufCount >= _buf.length) {
//            flushBuffer();
//        }
//        _buf[_bufCount++] = b;
//        _written++;
//    }

    /**
     * Writes <code>len</code> bytes from the specified byte array 
     * starting at offset <code>off</code> to this buffered output stream.
     *
     * <p> Ordinarily this method stores bytes from the given array into this
     * stream's buffer, flushing the buffer to the underlying output stream as
     * needed.  If the requested length is at least as large as this stream's
     * buffer, however, then this method will flush the buffer and write the
     * bytes directly to the underlying output stream.  Thus redundant
     * <code>BufferedOutputStream</code>s will not copy data unnecessarily.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void write(byte b[], int off, int len) throws IOException {
        _written += len;
        if (len >= _buf.length) {
            flushBuffer();
            _channel.write(ByteBuffer.wrap(b, off, len));
            return;
        }
        if (len > _buf.length - _bufCount) {
            flushBuffer();
        }
        System.arraycopy(b, off, _buf, _bufCount, len);
        _bufCount += len;
    }

    public final void write(byte b[]) throws IOException {
        _written += b.length;
        if (b.length >= _buf.length) {
            flushBuffer();
            _channel.write(ByteBuffer.wrap(b));
            return;
        }
        if (b.length > _buf.length - _bufCount) {
            flushBuffer();
        }
        System.arraycopy(b, 0, _buf, _bufCount, b.length);
        _bufCount += b.length;
    }

    /**
     * Flushes this buffered output stream. This forces any buffered 
     * output bytes to be written out to the underlying output stream. 
     *
     * @exception  IOException  if an I/O error occurs.
     */
    public final void flush() throws IOException {
        flushBuffer();
    }

    public final void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    /**
     * Writes out a <code>byte</code> to the underlying output stream as 
     * a 1-byte value. If no exception is thrown, the counter 
     * <code>written</code> is incremented by <code>1</code>.
     *
     * @param      v   a <code>byte</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeByte(int v) throws IOException {
        write(v);
    }

    /**
     * Writes a <code>short</code> to the underlying output stream as two
     * bytes, high byte first. If no exception is thrown, the counter 
     * <code>written</code> is incremented by <code>2</code>.
     *
     * @param      v   a <code>short</code> to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeShort(int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    /**
     * Writes a <code>char</code> to the underlying output stream as a 
     * 2-byte value, high byte first. If no exception is thrown, the 
     * counter <code>written</code> is incremented by <code>2</code>.
     *
     * @param      v   a <code>char</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeChar(int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    /**
     * Writes an <code>int</code> to the underlying output stream as four
     * bytes, high byte first. If no exception is thrown, the counter 
     * <code>written</code> is incremented by <code>4</code>.
     *
     * @param      v   an <code>int</code> to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeInt(int v) throws IOException {
//      write((v >>> 24) & 0xFF);
//      write((v >>> 16) & 0xFF);
//      write((v >>>  8) & 0xFF);
//      write((v >>>  0) & 0xFF);
        _b4[0] = (byte)(v >>> 24);
        _b4[1] = (byte)(v >>> 16);
        _b4[2] = (byte)(v >>>  8);
        _b4[3] = (byte)(v >>>  0);
        write(_b4);
    }

    /**
     * Writes a <code>long</code> to the underlying output stream as eight
     * bytes, high byte first. In no exception is thrown, the counter 
     * <code>written</code> is incremented by <code>8</code>.
     *
     * @param      v   a <code>long</code> to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeLong(long v) throws IOException {
        _b8[0] = (byte)(v >>> 56);
        _b8[1] = (byte)(v >>> 48);
        _b8[2] = (byte)(v >>> 40);
        _b8[3] = (byte)(v >>> 32);
        _b8[4] = (byte)(v >>> 24);
        _b8[5] = (byte)(v >>> 16);
        _b8[6] = (byte)(v >>>  8);
        _b8[7] = (byte)(v >>>  0);
        write(_b8);
    }

    /**
     * Converts the float argument to an <code>int</code> using the 
     * <code>floatToIntBits</code> method in class <code>Float</code>, 
     * and then writes that <code>int</code> value to the underlying 
     * output stream as a 4-byte quantity, high byte first. If no 
     * exception is thrown, the counter <code>written</code> is 
     * incremented by <code>4</code>.
     *
     * @param      v   a <code>float</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.lang.Float#floatToIntBits(float)
     */
    public final void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    /**
     * Converts the double argument to a <code>long</code> using the 
     * <code>doubleToLongBits</code> method in class <code>Double</code>, 
     * and then writes that <code>long</code> value to the underlying 
     * output stream as an 8-byte quantity, high byte first. If no 
     * exception is thrown, the counter <code>written</code> is 
     * incremented by <code>8</code>.
     *
     * @param      v   a <code>double</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.lang.Double#doubleToLongBits(double)
     */
    public final void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    /**
     * This operation is not supported.<br>
     * Writes out the string to the underlying output stream as a 
     * sequence of bytes. Each character in the string is written out, in 
     * sequence, by discarding its high eight bits. If no exception is 
     * thrown, the counter <code>written</code> is incremented by the 
     * length of <code>s</code>.
     *
     * @param      s   a string of bytes to be written.
//     * @exception  IOException  if an I/O error occurs.
     */
    public final void writeBytes(String s) {
        throw new UnsupportedOperationException(s);
        //Should not be used, looses significant byte.
        //TODO use for classnames?
//      int len = s.length();
//      s.getBytes();
//      for (int i = 0 ; i < len ; i++) {
//      write((byte)s.charAt(i));
//      }
    }

    private static final int SIZE_B_STRING = 1399;
    private final byte[] _bString = new byte[SIZE_B_STRING * 2];

    /**
     * Writes a string to the underlying output stream as a sequence of 
     * characters. Each character is written to the data output stream as 
     * if by the <code>writeChar</code> method. If no exception is 
     * thrown, the counter <code>written</code> is incremented by twice 
     * the length of <code>s</code>.
     *
     * @param      s   a <code>String</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     * @see        java.io.DataOutputStream#writeChar(int)
     */
    public final void writeChars(String s) throws IOException {
        int len = s.length();
//      for (int i = 0 ; i < len ; i++) {
//      int v = s.charAt(i);
//      write((v >>> 8) & 0xFF); 
//      write((v >>> 0) & 0xFF); 
//      }
        //The following reduced the execution time by 85%.
        int charCount = 0;
        int byteCount = 0;
        while (charCount < len) {
            if (SIZE_B_STRING + charCount < len) {
                byteCount = 0;
                for (int i = 0; i < SIZE_B_STRING; i++) {
                    int v = s.charAt(charCount++);
                    _bString[byteCount++] = (byte) ((v >>> 8) & 0xFF);
                    _bString[byteCount++] = (byte) ((v >>> 0) & 0xFF);
                }
                write(_bString);
            } else {
                byteCount = 0;
                for (int i = charCount; i < len; i++) {
                    int v = s.charAt(charCount++);
                    _bString[byteCount++] = (byte) ((v >>> 8) & 0xFF); 
                    _bString[byteCount++] = (byte) ((v >>> 0) & 0xFF); 
                }
                write(_bString, 0, byteCount);
            }
        }
    }

    /** 
     * Number of written bytes.
     * @return Number of written bytes.
     */
    public final long size() {
        return _written;
    }

    /** 
     * This operation is not supported.
     */
    public void writeUTF(String str) {
        throw new UnsupportedOperationException(str);
    }
}
