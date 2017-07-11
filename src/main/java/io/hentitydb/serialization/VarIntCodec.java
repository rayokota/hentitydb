package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores integers as variable-length encoded bytes.
 */
public class VarIntCodec extends AbstractCodec<Integer> {
    @Override
    public int expectedSize(Integer value) {
        return 5;
    }

    @Override
    public void encode(Integer value, WriteBuffer buffer) {
        buffer.writeVarInt(value);
    }

    @Override
    public Integer decode(ReadBuffer buffer) {
        return buffer.readVarInt();
    }
}
