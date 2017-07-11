package io.hentitydb.store;

import io.hentitydb.serialization.Codec;

public class KeyColumn<K, C> {

    private final Codec<K> keyCodec;
    private final byte[] rawKey;
    private final Column<C> column;

    public KeyColumn(Codec<K> keyCodec, byte[] rawKey, Column<C> column) {
        this.keyCodec = keyCodec;
        this.rawKey = rawKey;
        this.column = column;
    }

    /**
     * Returns the key codec.
     *
     * @return the key codec
     */
    public Codec<K> getKeyCodec() {
        return keyCodec;
    }

    /**
     * Returns the row key.
     *
     * @return the row key
     */
    public K getKey() {
        return keyCodec.decode(rawKey);
    }

    /**
     * Returns the raw bytes of the row key.
     *
     * @return the row key as bytes
     */
    public byte[] getRawKey() {
        return rawKey;
    }

    /**
     * Returns the column.
     *
     * @return the column
     */
    public Column<C> getColumn() {
        return column;
    }
}
