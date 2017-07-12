package io.hentitydb.serialization;

import com.google.common.base.Charsets;

import static io.hentitydb.serialization.BufferRecycler.recycler;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;

/**
 * A write buffer for serializing values.
 */
public class WriteBuffer {
    private byte[] buf;
    private int count;

    /**
     * Creates a new {@link WriteBuffer} with the given expected initial capacity.
     *
     * @param initialCapacity the expected size of the buffer
     */
    public WriteBuffer(int initialCapacity) {
        this.buf = recycler().allocate(initialCapacity);
        this.count = 0;
    }

    protected byte[] getRawBytes() {
        return buf;
    }

    /**
     * Writes a single byte to the buffer.
     *
     * @param v a byte
     */
    public void writeByte(int v) {
        ensureCapacity(count + 1);
        buf[count++] = (byte) v;
    }

    /**
     * Writes a short as a 16-bit, fixed-length, big-endian value.
     *
     * @param v a short
     */
    public void writeShort(int v) {
        ensureCapacity(count + 2);
        buf[count++] = (byte) (v >>> 8);
        buf[count++] = (byte) v;
    }

    /**
     * Writes an int as a 32-bit, fixed-length, big-endian value.
     *
     * @param v an int
     */
    public void writeInt(int v) {
        ensureCapacity(count + 4);
        buf[count++] = (byte) (v >>> 24);
        buf[count++] = (byte) (v >>> 16);
        buf[count++] = (byte) (v >>> 8);
        buf[count++] = (byte) v;
    }

    /**
     * Writes a float as a 32-bit value.
     *
     * @param v    a float
     */
    public void writeFloat(float v) {
        writeInt(Float.floatToIntBits(v));
    }

    /**
     * Writes a double as a 64-bit value.
     *
     * @param v    a double
     */
    public void writeDouble(double v) {
        writeLong(Double.doubleToLongBits(v));
    }

    /**
     * Writes a long as a 64-bit, fixed-length, big-endian value.
     *
     * @param v a long
     */
    public void writeLong(long v) {
        ensureCapacity(count + 8);
        buf[count++] = (byte) (v >>> 56);
        buf[count++] = (byte) (v >>> 48);
        buf[count++] = (byte) (v >>> 40);
        buf[count++] = (byte) (v >>> 32);
        buf[count++] = (byte) (v >>> 24);
        buf[count++] = (byte) (v >>> 16);
        buf[count++] = (byte) (v >>> 8);
        buf[count++] = (byte) v;
    }

    /**
     * Writes an int as a variable-length value.
     *
     * @param v an int
     */
    public void writeVarInt(int v) {
        ensureCapacity(count + 5);
        long n = ((long) v << 1) ^ ((long) v >> 31); // move sign to low-order bit
        while ((n & ~0x7f) != 0) {
            buf[count++] = (byte) ((n & 0x7f) | 0x80);
            n >>>= 7;
        }
        buf[count++] = (byte) n;
    }

    /**
     * Writes a long as a variable-length value.
     *
     * @param v a long
     */
    public void writeVarLong(long v) {
        ensureCapacity(count + 9);
        long n = (v << 1) ^ (v >> 63); // move sign to low-order bit
        while ((n & ~0x7f) != 0) {
            buf[count++] = (byte) ((n & 0x7f) | 0x80);
            n >>>= 7;
        }
        buf[count++] = (byte) n;
    }

    /**
     * Writes the contents of the given byte array.
     *
     * @param b a byte array
     */
    public void writeBytes(byte[] b) {
        writeBytes(b, 0, b.length);
    }

    /**
     * Writes the slice of the given byte array.
     *
     * @param b a byte array
     * @param offset the offset index into {@code b}
     * @param length the number of bytes of {@code b} to write
     */
    public void writeBytes(byte[] b, int offset, int length) {
        final int newCount = count + length;
        ensureCapacity(newCount);
        arraycopy(b, offset, buf, count, length);
        this.count = newCount;
    }

    /**
     * Write a string as a UTF8-encoded byte sequence.
     *
     * @param s a string
     */
    public void writeUtf8String(String s) {
        writeUtf8String(s, true);
    }

    /**
     * Write a string as a UTF8-encoded byte sequence.
     *
     * @param encodeLength whether to write the length
     * @param s a string
     */
    public void writeUtf8String(String s, boolean encodeLength) {
        final byte[] bytes = s.getBytes(Charsets.UTF_8);
        if (encodeLength) writeVarInt(bytes.length);
        writeBytes(bytes);
    }

    /**
     * Releases any intermediate buffers and returns the final byte array.
     *
     * @return a byte array containing all writes made to the buffer
     */
    public byte[] finish() {
        final byte[] bytes = new byte[count];
        arraycopy(buf, 0, bytes, 0, count);
        recycler().release(buf);
        return bytes;
    }

    private byte[] ensureCapacity(int requiredSize) {
        if (buf.length < requiredSize) {
            final byte[] newBuf = recycler().allocate(max(buf.length << 1, requiredSize));
            arraycopy(buf, 0, newBuf, 0, count);
            final byte[] oldBuf = buf;
            buf = newBuf;
            recycler().release(oldBuf);
        }
        return buf;
    }
}
