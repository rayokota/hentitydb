package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link Boolean}s.
 */
public class BooleanCodec extends AbstractCodec<Boolean> {
    @Override
    public int expectedSize(Boolean value) {
        return 1;
    }

    @Override
    public void encode(Boolean value, WriteBuffer buffer) {
        buffer.writeByte(value ? (byte) -1 : (byte) 0);
    }

    @Override
    public Boolean decode(ReadBuffer buffer) {
        return buffer.readByte() != (byte) 0;
    }
}
