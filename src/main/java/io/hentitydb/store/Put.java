package io.hentitydb.store;

import io.hentitydb.serialization.Codec;

import java.util.Date;

public interface Put<K, C> extends RowMutation<K, C> {

    /**
     * Specifies a column to be set as a boolean.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, boolean value);

    /**
     * Specifies a column to be set as a boolean.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, boolean value);

    /**
     * Specifies a column to be set as a short.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, short value);

    /**
     * Specifies a column to be set as a short.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, short value);

    /**
     * Specifies a column to be set as an integer.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, int value);

    /**
     * Specifies a column to be set as an integer.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, int value);

    /**
     * Specifies a column to be set as a long.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, long value);

    /**
     * Specifies a column to be set as a long.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, long value);

    /**
     * Specifies a column to be set as a Date.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, Date value);

    /**
     * Specifies a column to be set as a Date.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, Date value);

    /**
     * Specifies a column to be set as a float.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, float value);

    /**
     * Specifies a column to be set as a float.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, float value);

    /**
     * Specifies a column to be set as a double.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, double value);

    /**
     * Specifies a column to be set as a double.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, double value);

    /**
     * Specifies a column to be set as a byte array.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, byte[] value);

    /**
     * Specifies a column to be set as a byte array.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, byte[] value);

    /**
     * Specifies a column to be set as a String.
     *
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(C column, String value);

    /**
     * Specifies a column to be set as a String.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @return the put operation
     */
    Put<K, C> addColumn(String family, C column, String value);

    /**
     * Specifies a column to be set.
     *
     * @param column the column name
     * @param value the value to set
     * @param valueCodec the value codec
     * @return the put operation
     */
    <V> Put<K, C> addColumn(C column, V value, Codec<V> valueCodec);

    /**
     * Specifies a column to be set.
     *
     * @param family the column family
     * @param column the column name
     * @param value the value to set
     * @param valueCodec the value codec
     * @return the put operation
     */
    <V> Put<K, C> addColumn(String family, C column, V value, Codec<V> valueCodec);
}
