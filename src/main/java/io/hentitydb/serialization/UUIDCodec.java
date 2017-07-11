package io.hentitydb.serialization;

import java.util.UUID;

/**
 * A {@link Codec} implementation which stores {@link UUID}s.
 */
public class UUIDCodec extends AbstractCodec<UUID> {
    @Override
    public int expectedSize(UUID value) {
        return 16;
    }

    @Override
    public void encode(UUID value, WriteBuffer buffer) {
        long msb = value.getMostSignificantBits();
        long lsb = value.getLeastSignificantBits();
        buffer.writeLong(msb);
        buffer.writeLong(lsb);
    }

    @Override
    public UUID decode(ReadBuffer buffer) {
        return new UUID(buffer.readLong(), buffer.readLong());
    }
}
