package io.hentitydb.serialization;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A {@link Codec} implementation which stores {@link BigDecimal}s.
 */
public class BigDecimalCodec extends VarLengthCodec<BigDecimal> {

    public BigDecimalCodec() {
        super();
    }

    public BigDecimalCodec(boolean encodeLength) {
        super(encodeLength);
    }

    @Override
    public void encode(BigDecimal value, WriteBuffer buffer) {
        buffer.writeInt(value.scale());
        final byte[] valueBytes = value.unscaledValue().toByteArray();
        if (getEncodeLength()) buffer.writeVarInt(valueBytes.length);
        buffer.writeBytes(valueBytes);
    }

    @Override
    public BigDecimal decode(ReadBuffer buffer) {
        final int scale = buffer.readInt();
        final byte[] valueBytes;
        if (getEncodeLength()) {
            final int size = buffer.readVarInt();
            valueBytes = buffer.readBytes(size);
        } else {
            valueBytes = buffer.readBytes();
        }
        return new BigDecimal(new BigInteger(valueBytes), scale);
    }
}
