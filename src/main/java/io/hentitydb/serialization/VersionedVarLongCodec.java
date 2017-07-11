package io.hentitydb.serialization;

/**
 * A versioned {@link Codec} implementation which stores longs as variable-length encoded bytes.
 */
public class VersionedVarLongCodec extends AbstractVersionedCodec<Long> {

    @Override
    public int getLatestVersion() {
        return 1;
    }

    @Override
    protected void encode(int version, final Long value, final WriteBuffer buffer) {
        buffer.writeVarLong(value);
    }

    @Override
    protected Long decode(int version, final ReadBuffer buffer) {
        return buffer.readVarLong();
    }
}
