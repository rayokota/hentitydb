package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link Integer}s as 4 bytes, big-endian.
 */
public class IntCodec extends AbstractCodec<Integer> {
    @Override
    public int expectedSize(Integer value) {
        return 4;
    }

    @Override
    public void encode(Integer value, WriteBuffer buffer) {
        buffer.writeInt(value);
    }

    @Override
    public Integer decode(ReadBuffer buffer) {
        return buffer.readInt();
    }
}
