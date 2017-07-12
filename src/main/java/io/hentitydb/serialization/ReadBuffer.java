package io.hentitydb.serialization;

import com.google.common.base.Charsets;

import static java.lang.System.arraycopy;

/**
 * A read buffer for deserializing values.
 */
public class ReadBuffer {
    private final byte[] buf;
    private int pos;

    /**
     * Creates a new read buffer for a byte array.
     *
     * @param buf an array of bytes
     */
    public ReadBuffer(byte[] buf) {
        this.buf = buf;
        this.pos = 0;
    }

    protected byte[] getRawBytes() {
        return buf;
    }

    /**
     * Reads the given number of bytes from the buffer returns then as a byte array.
     *
     * @param count the number of bytes to read
     * @return a byte array filled with {@code count} bytes from the buffer
     */
    public byte[] readBytes(int count) {
        final int len = Math.min(count, buf.length - pos);
        final byte[] bytes = new byte[len];
        arraycopy(buf, pos, bytes, 0, len);
        pos += len;
        return bytes;
    }

    /**
     * Reads the entire buffer as a byte array.
     *
     * @return a byte array filled with bytes from the buffer
     */
    public byte[] readBytes() {
        return readBytes(buf.length - pos);
    }

    /**
     * Reads a UTF8-encoded byte series as a string.
     *
     * @return a string
     */
    public String readUtf8String() {
        return readUtf8String(true);
    }

    /**
     * Reads a UTF8-encoded byte series as a string.
     *
     * @param encodeLength whether to read the length
     * @return a string
     */
    public String readUtf8String(boolean encodeLength) {
        final byte[] bytes;
        if (encodeLength) {
            final int len = readVarInt();
            bytes = readBytes(len);
        } else {
            bytes = readBytes();
        }
        return new String(bytes, Charsets.UTF_8);
    }

    /**
     * Reads a byte from the buffer.
     *
     * @return a single byte
     */
    public byte readByte() {
        return buf[pos++];
    }

    /**
     * Reads a fixed-length, 16-bit, big-endian short from the buffer.
     *
     * @return a 16-bit, signed integer
     */
    public short readShort() {
        return (short) ((((int) buf[pos++] & 0xFF) << 8) +
                         ((int) buf[pos++] & 0xFF));
    }

    /**
     * Reads a fixed-length, 32-bit, big-endian int from the buffer.
     *
     * @return a 32-bit, signed integer
     */
    public int readInt() {
        return ((buf[pos++] & 0xFF) << 24) +
                ((buf[pos++] & 0xFF) << 16) +
                ((buf[pos++] & 0xFF) << 8) +
                (buf[pos++] & 0xFF);
    }

    /**
     * Reads a fixed-length, 64-bit, big-endian long from the buffer.
     *
     * @return a 64-bit, signed integer
     */
    public long readLong() {
        return ((long) (buf[pos++] & 0xFF) << 56) +
                ((long) (buf[pos++] & 0xFF) << 48) +
                ((long) (buf[pos++] & 0xFF) << 40) +
                ((long) (buf[pos++] & 0xFF) << 32) +
                ((long) (buf[pos++] & 0xFF) << 24) +
                ((buf[pos++] & 0xFF) << 16) +
                ((buf[pos++] & 0xFF) << 8) +
                (buf[pos++] & 0xFF);
    }

    /**
     * Reads a varint-encoded 32-bit int from the buffer.
     *
     * @return a 32-bit, signed integer
     */
    public int readVarInt() {
        int b = buf[pos++] & 0xff;
        int n = b & 0x7f;
        if (b > 0x7f) {
            b = buf[pos++] & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buf[pos++] & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buf[pos++] & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = buf[pos++] & 0xff;
                        n ^= (b & 0x7f) << 28;
                    }
                    if (b > 0x7f) {
                        throw new RuntimeException("Invalid int encoding");
                    }
                }
            }
        }
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    /**
     * Reads a varint-encoded 64-bit long from the buffer.
     *
     * @return a 64-bit, signed integer
     */
    public long readVarLong() {
        long b = buf[pos++] & 0xff;
        long n = b & 0x7f;
        if (b > 0x7f) {
            b = buf[pos++] & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buf[pos++] & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buf[pos++] & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = buf[pos++] & 0xff;
                        n ^= (b & 0x7f) << 28;
                        if (b > 0x7f) {
                            b = buf[pos++] & 0xff;
                            n ^= (b & 0x7f) << 35;
                            if (b > 0x7f) {
                                b = buf[pos++] & 0xff;
                                n ^= (b & 0x7f) << 42;
                                if (b > 0x7f) {
                                    b = buf[pos++] & 0xff;
                                    n ^= (b & 0x7f) << 49;
                                    if (b > 0x7f) {
                                        b = buf[pos++] & 0xff;
                                        n ^= (b & 0x7f) << 56;
                                        if (b > 0x7f) {
                                            b = buf[pos++] & 0xff;
                                            n ^= (b & 0x7f) << 63;
                                            if (b > 0x7f) {
                                                throw new RuntimeException("Invalid int encoding");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    /**
     * Reads a 32-bit floating-point number.
     *
     * @return a 32-bit floating-point number
     */
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    /**
     * Reads a 64-bit floating-point number.
     *
     * @return a 64-bit floating-point number
     */
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }
}
