package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link Integer}s as 4 bytes, big-endian,
 * in descending order.
 */
public class InvertedIntCodec extends AbstractCodec<Integer> {
    @Override
    public int expectedSize(Integer value) {
        return 4;
    }

    @Override
    public void encode(Integer value, WriteBuffer buffer) {
        buffer.writeInt(Integer.MAX_VALUE - value);
    }

    @Override
    public Integer decode(ReadBuffer buffer) {
        return Integer.MAX_VALUE - buffer.readInt();
    }
}
