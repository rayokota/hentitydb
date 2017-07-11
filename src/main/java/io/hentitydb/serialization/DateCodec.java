package io.hentitydb.serialization;

import java.util.Date;

/**
 * A {@link Codec} implementation which stores {@link Date}s.
 */
public class DateCodec extends AbstractCodec<Date> {
    @Override
    public int expectedSize(Date value) {
        return 8;
    }

    @Override
    public void encode(Date value, WriteBuffer buffer) {
        buffer.writeLong(value.getTime());
    }

    @Override
    public Date decode(ReadBuffer buffer) {
        return new Date(buffer.readLong());
    }
}
