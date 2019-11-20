package io.hentitydb.serialization;

import java.util.Arrays;

/**
 * A codec which prepends a one-byte salt to a value.
 *
 * @param <T>    the value type
 */
public class SaltingCodec<T> extends AbstractCodec<T> {
    private final Codec<T> codec;
    private final int numBuckets;

    public static final int DEFAULT_NUM_BUCKETS = 256;

    private static final byte[][] PREFIXES;
    static {
        PREFIXES = new byte[256][];
        for (int i = 0; i < 256; i++) {
            PREFIXES[i] = new byte[] {(byte) i};
        }
    }

    /**
     * Create a new {@link SaltingCodec} for the given codec.
     *
     * @param codec       the underlying codec
     */
    public SaltingCodec(Codec<T> codec) {
        this(codec, DEFAULT_NUM_BUCKETS);
    }

    /**
     * Create a new {@link SaltingCodec} for the given codec.
     *
     * @param codec       the underlying codec
     * @param numBuckets  the number of buckets for the salt, between 1 and 256.
     */
    public SaltingCodec(Codec<T> codec, int numBuckets) {
        if (numBuckets < 1 || numBuckets > 256) throw new IllegalArgumentException("numBuckets must be between 1 and 256");
        this.codec = codec;
        this.numBuckets = numBuckets;
    }

    public Codec<T> getCodec() {
        return codec;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public int expectedSize(T value) {
        return codec.expectedSize(value) + 1;
    }

    public void encode(T value, WriteBuffer buffer) {
        byte[] bytes = codec.encode(value);
        buffer.writeByte(getSaltingByte(bytes));
        buffer.writeBytes(bytes);
    }

    @Override
    public T decode(ReadBuffer buffer) {
        buffer.readByte();
        final T value = codec.decode(buffer);
        return value;
    }

    /**
     * Returns the salt for a given value.
     *
     * @param value    the value
     * @return the salt to prepend to {@code value}
     */
    protected byte getSaltingByte(byte[] value) {
        int hash = calculateHashCode(value);
        byte bucketByte = (byte) (Math.abs(hash) % numBuckets);
        return bucketByte;
    }

    private static int calculateHashCode(byte[] a) {
        if (a == null) return 0;
        int result = 1;
        for (byte b : a) {
            result = 31 * result + b;
        }
        return result;
    }

    @Override
    public Codec<Codec<T>> getSelfCodec() {
        return new SaltingCodecCodec<>();
    }

    public byte[][] getAllSaltingPrefixes() {
        return Arrays.copyOfRange(PREFIXES, 0, numBuckets);
    }
}
