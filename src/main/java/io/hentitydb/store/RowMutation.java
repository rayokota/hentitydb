package io.hentitydb.store;

import io.hentitydb.serialization.Codec;

import java.util.Date;

public interface RowMutation<K, C> extends RowOperation<K, C> {

    /**
     * Executes the row mutation.
     */
    void execute();

    /**
     * Executes the row mutation only if the given column does not exist.
     *
     * @param column the column name
     * @return the operation
     */
    boolean executeIfAbsent(C column);

    /**
     * Executes the row mutation only if the given column does not exist.
     *
     * @param family the column family
     * @param column the column name
     * @return the operation
     */
    boolean executeIfAbsent(String family, C column);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, boolean value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, boolean value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, short value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, short value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, int value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, int value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, long value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, long value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, Date value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, Date value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, float value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, float value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, double value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, double value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, byte[] value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, byte[] value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(C column, CompareOp compareOp, String value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @return the operation
     */
    boolean executeIf(String family, C column, CompareOp compareOp, String value);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @param valueCodec the value codec
     * @return the operation
     */
    <V> boolean executeIf(C column, CompareOp compareOp, V value, Codec<V> valueCodec);

    /**
     * Executes the row mutation only if the given column equals the given value.
     *
     * @param family the column family
     * @param column the column name
     * @param compareOp
     * @param value the value to check
     * @param valueCodec the value codec
     * @return the operation
     */
    <V> boolean executeIf(String family, C column, CompareOp compareOp, V value, Codec<V> valueCodec);

    /**
     * Sets the TTL for the result of the mutation, in milliseconds.
     *
     * @param ttl the TTL, in milliseconds
     * @return the operation
     */
    RowMutation<K, C> setTTL(int ttl);
}
