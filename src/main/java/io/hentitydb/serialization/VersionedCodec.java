package io.hentitydb.serialization;

/**
 * A generic codec for encoding and decoding objects as byte arrays that supports versioning.
 *
 * @param <T> the type of values to be encoded or decoded
 */
public interface VersionedCodec<T> extends Codec<T> {

    /**
     * Returns the latest version number for this codec.
     *
     * @return the latest version number
     */
    int getLatestVersion();
}
