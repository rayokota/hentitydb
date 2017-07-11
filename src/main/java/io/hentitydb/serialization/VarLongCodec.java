package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores longs as variable-length encoded bytes.
 */
public class VarLongCodec extends AbstractCodec<Long> {
    @Override
    public int expectedSize(Long value) {
        return 9;
    }

    @Override
    public void encode(Long value, WriteBuffer buffer) {
        buffer.writeVarLong(value);
    }

    @Override
    public Long decode(ReadBuffer buffer) {
        return buffer.readVarLong();
    }
}
