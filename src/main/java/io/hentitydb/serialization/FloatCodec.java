package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link Float}s.
 */
public class FloatCodec extends AbstractCodec<Float> {
    @Override
    public int expectedSize(Float value) {
        return 4;
    }

    @Override
    public void encode(Float value, WriteBuffer buffer) {
        buffer.writeFloat(value);
    }

    @Override
    public Float decode(ReadBuffer buffer) {
        return buffer.readFloat();
    }
}
