package io.hentitydb.serialization;

/**
 * A generic codec for encoding and decoding objects as byte arrays.
 *
 * @param <T> the type of values to be encoded or decoded
 */
public interface Codec<T> {
    /**
     * Returns the estimated size of the given value as a byte array.
     *
     * @param value a value which will be encoded
     * @return the number of bytes which {@code value} is expected to take when serialized
     */
    int expectedSize(T value);

    /**
     * Encodes the given value and stores the encoded contents in the provided {@link WriteBuffer}.
     *
     * @param value the value to be encoded
     * @param buffer the buffer to which {@code value} will be encoded
     */
    void encode(T value, WriteBuffer buffer);

    /**
     * Encodes the given value and returns the encoded form as a byte array.
     *
     * @param value the value to be encoded
     * @return the encoded form of {@code value}
     */
    byte[] encode(T value);

    /**
     * Decodes a value from the given read buffer.
     *
     * @param buffer a {@link ReadBuffer} containing the encoded form
     * @return the decoded instance
     */
    T decode(ReadBuffer buffer);

    /**
     * Decodes a value from the given byte array.
     *
     * @param bytes a byte array containing the encoded form
     * @return the decoded instance
     */
    T decode(byte[] bytes);

    /**
     * Returns a codec to serialize and deserialize this codec.
     *
     * @return the self codec
     */
    Codec<Codec<T>> getSelfCodec();

}
