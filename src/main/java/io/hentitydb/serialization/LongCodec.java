package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link Long}s as 8 bytes, big-endian.
 */
public class LongCodec extends AbstractCodec<Long> {
    @Override
    public int expectedSize(Long value) {
        return 8;
    }

    @Override
    public void encode(Long value, WriteBuffer buffer) {
        buffer.writeLong(value);
    }

    @Override
    public Long decode(ReadBuffer buffer) {
        return buffer.readLong();
    }
}
