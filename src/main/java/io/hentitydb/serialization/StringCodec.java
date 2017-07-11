package io.hentitydb.serialization;

/**
 * A {@link Codec} implementation which stores UTF-8 strings.
 */
public class StringCodec extends VarLengthCodec<String> {

    public StringCodec() {
        super();
    }

    public StringCodec(boolean encodeLength) {
        super(encodeLength);
    }

    @Override
    public void encode(String value, WriteBuffer buffer) {
        buffer.writeUtf8String(value, getEncodeLength());
    }

    @Override
    public String decode(ReadBuffer buffer) {
        return buffer.readUtf8String(getEncodeLength());
    }
}
