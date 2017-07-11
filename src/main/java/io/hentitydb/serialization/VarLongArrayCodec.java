package io.hentitydb.serialization;

/**
 * A codec which stores arrays of {@code long}s as delta-compressed, variable-length encoded bytes.
 */
public class VarLongArrayCodec extends AbstractCodec<long[]> {
    @Override
    public int expectedSize(long[] value) {
        return value.length * 7;
    }

    @Override
    public void encode(long[] value, WriteBuffer buffer) {
        buffer.writeVarInt(value.length);
        long last = 0;
        for (long v : value) {
            buffer.writeVarLong(last - v);
            last = v;
        }
    }

    @Override
    public long[] decode(ReadBuffer buffer) {
        final int size = buffer.readVarInt();
        final long[] values = new long[size];
        long last = 0;
        for (int i = 0; i < size; i++) {
            values[i] = last = last - buffer.readVarLong();
        }
        return values;
    }
}
