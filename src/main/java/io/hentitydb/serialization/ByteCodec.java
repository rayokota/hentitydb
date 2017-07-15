package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link Byte}s.
 */
public class ByteCodec extends AbstractCodec<Byte> {
    @Override
    public int expectedSize(Byte value) {
        return 1;
    }

    @Override
    public void encode(Byte value, WriteBuffer buffer) {
        buffer.writeByte(value);
    }

    @Override
    public Byte decode(ReadBuffer buffer) {
        return buffer.readByte();
    }
}
