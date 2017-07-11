package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores {@link Double}s.
 */
public class DoubleCodec extends AbstractCodec<Double> {
    @Override
    public int expectedSize(Double value) {
        return 8;
    }

    @Override
    public void encode(Double value, WriteBuffer buffer) {
        buffer.writeDouble(value);
    }

    @Override
    public Double decode(ReadBuffer buffer) {
        return buffer.readDouble();
    }
}
