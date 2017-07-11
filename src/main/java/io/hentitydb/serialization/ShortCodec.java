package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link Short}s.
 */
public class ShortCodec extends AbstractCodec<Short> {
    @Override
    public int expectedSize(Short value) {
        return 2;
    }

    @Override
    public void encode(Short value, WriteBuffer buffer) {
        buffer.writeShort(value);
    }

    @Override
    public Short decode(ReadBuffer buffer) {
        return buffer.readShort();
    }
}
