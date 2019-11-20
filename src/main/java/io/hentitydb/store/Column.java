package io.hentitydb.store;

import io.hentitydb.serialization.Codec;

import java.util.Date;

public interface Column<C> {

    /**
     * Returns the column codec.
     *
     * @return the column codec
     */
    Codec<C> getColumnCodec();

    /**
     * Returns the name of the column family.
     *
     * @return the column family
     */
    String getFamily();

    /**
     * Returns the name of the column.
     *
     * @return the column name
     */
    C getName();

    /**
     * Returns the raw bytes of the column name.
     *
     * @return the column name as bytes
     */
    byte[] getRawName();

    /**
     * Returns the column value as a boolean.
     *
     * @return the column value
     */
    boolean getBoolean();

    /**
     * Returns the column value as a short.
     *
     * @return the column value
     */
    short getShort();

    /**
     * Returns the column value as an int.
     *
     * @return the column value
     */
    int getInt();

    /**
     * Returns the column value as a long.
     *
     * @return the column value
     */
    long getLong();

    /**
     * Returns the column value as a Date.
     *
     * @return the column value
     */
    Date getDate();

    /**
     * Returns the column value as a float.
     *
     * @return the column value
     */
    float getFloat();

    /**
     * Returns the column value as a double.
     *
     * @return the column value
     */
    double getDouble();

    /**
     * Returns the column value as a byte array.
     *
     * @return the column value
     */
    byte[] getBytes();

    /**
     * Returns the column value as a String.
     *
     * @return the column value
     */
    String getString();

    /**
     * Returns the column value.
     *
     * @param <V> the codec type
     * @param valueCodec the value codec
     * @return the column value
     */
    <V> V getValue(Codec<V> valueCodec);

    /**
     * Returns the column timestamp.
     *
     * @return the column timestamp
     */
    long getTimestamp();
}
