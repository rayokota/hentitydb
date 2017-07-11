package io.hentitydb.serialization;

/**
 * A codec which stores arrays of {@code int}s as delta-compressed, variable-length encoded bytes.
 */
public class VarIntArrayCodec extends AbstractCodec<int[]> {
    @Override
    public int expectedSize(int[] value) {
        return value.length * 3;
    }

    @Override
    public void encode(int[] value, WriteBuffer buffer) {
        buffer.writeVarInt(value.length);
        int last = 0;
        for (int v : value) {
            buffer.writeVarInt(last - v);
            last = v;
        }
    }

    @Override
    public int[] decode(ReadBuffer buffer) {
        final int size = buffer.readVarInt();
        final int[] values = new int[size];
        int last = 0;
        for (int i = 0; i < size; i++) {
            values[i] = last = last - buffer.readVarInt();
        }
        return values;
    }
}
