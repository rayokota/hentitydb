package io.hentitydb.serialization;

/**
 * A base class for {@link Codec} implementations that support versioning.
 *
 * @param <T> the type of values to be encoded or decoded
 */
public abstract class AbstractVersionedCodec<T> extends AbstractCodec<T> implements VersionedCodec<T> {
    private static final int DEFAULT_EXPECTED_SIZE = 16 * 1024;

    public abstract int getLatestVersion();

    @Override
    public int expectedSize(T value) {
        return DEFAULT_EXPECTED_SIZE;
    }

    @Override
    public byte[] encode(T value) {
        return encode(getLatestVersion(), value);
    }

    public byte[] encode(int version, T value) {
        if (value == null) return null;
        final WriteBuffer buffer = new WriteBuffer(expectedSize(value));
        buffer.writeVarInt(version);
        encode(version, value, buffer);
        return buffer.finish();
    }

    @Override
    public void encode(T value, WriteBuffer buffer) {
        int version = getLatestVersion();
        buffer.writeVarInt(version);
        encode(version, value, buffer);
    }

    protected abstract void encode(int version, T value, WriteBuffer buffer);

    @Override
    public T decode(byte[] bytes) {
        if (bytes == null) return null;
        final ReadBuffer buffer = new ReadBuffer(bytes);
        int version = buffer.readVarInt();
        return decode(version, buffer);
    }

    @Override
    public T decode(ReadBuffer buffer) {
        int version = buffer.readVarInt();
        return decode(version, buffer);
    }

    protected abstract T decode(int version, ReadBuffer buffer);

    @Override
    public Codec<Codec<T>> getSelfCodec() {
        return new CodecCodec<>();
    }
}
