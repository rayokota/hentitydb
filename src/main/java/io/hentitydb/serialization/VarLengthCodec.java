package io.hentitydb.serialization;

/**
 * A base class for {@link Codec} implementations that have a variable length.
 *
 * @param <T> the type of values to be encoded or decoded
 */
public abstract class VarLengthCodec<T> extends AbstractCodec<T> {
    private final boolean encodeLength;

    public VarLengthCodec() {
        encodeLength = false;
    }

    public VarLengthCodec(boolean encodeLength) {
        this.encodeLength = encodeLength;
    }

    public boolean getEncodeLength() {
        return encodeLength;
    }

    @Override
    public Codec<Codec<T>> getSelfCodec() {
        return new VarLengthCodecCodec<>();
    }
}
