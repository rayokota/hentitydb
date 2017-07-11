package io.hentitydb.serialization;

/**
 * A base class for {@link Codec} implementations.
 *
 * @param <T> the type of values to be encoded or decoded
 */
public abstract class AbstractCodec<T> implements Codec<T> {
    private static final int DEFAULT_EXPECTED_SIZE = 16 * 1024;

    @Override
    public int expectedSize(T value) {
        return DEFAULT_EXPECTED_SIZE;
    }

    @Override
    public byte[] encode(T value) {
        if (value == null) return null;
        final WriteBuffer buffer = new WriteBuffer(expectedSize(value));
        encode(value, buffer);
        return buffer.finish();
    }

    @Override
    public T decode(byte[] bytes) {
        if (bytes == null) return null;
        return decode(new ReadBuffer(bytes));
    }

    @Override
    public Codec<Codec<T>> getSelfCodec() {
        return new CodecCodec<>();
    }
}
