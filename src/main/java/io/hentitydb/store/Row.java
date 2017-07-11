package io.hentitydb.store;

import io.hentitydb.serialization.Codec;

import java.util.Date;
import java.util.List;

public interface Row<K, C> {

    /**
     * Returns the row key.
     *
     * @return the row key
     */
    K getKey();

    /**
     * Returns the raw bytes of the row key.
     *
     * @return the row key as bytes
     */
    byte[] getRawKey();

    /**
     * Returns the row columns.
     *
     * @return a list of row columns
     */
    List<Column<C>> getColumns();

    /**
     * Determines whether the row is empty.
     *
     * @return true if the row is empty; otherwise false
     */
    boolean isEmpty();

    /**
     * Returns the number of columns.
     *
     * @return the size
     */
    int size();

    /**
     * Returns the column with the given name.
     *
     * @param name the column name
     * @return a column
     */
    Column<C> getColumn(C name);

    /**
     * Returns the column with the given name.
     *
     * @param family the column family
     * @param name the column name
     * @return a column
     */
    Column<C> getColumn(String family, C name);

    /**
     * Returns the column value for the given column as a boolean.
     *
     * @param name the column name
     * @return the column value
     */
    boolean getBoolean(C name);

    /**
     * Returns the column value for the given column as a boolean.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    boolean getBoolean(String family, C name);

    /**
     * Returns the column value for the given column as a short.
     *
     * @param name the column name
     * @return the column value
     */
    short getShort(C name);

    /**
     * Returns the column value for the given column as a short.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    short getShort(String family, C name);

    /**
     * Returns the column value for the given column as an integer.
     *
     * @param name the column name
     * @return the column value
     */
    int getInt(C name);

    /**
     * Returns the column value for the given column as an integer.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    int getInt(String family, C name);

    /**
     * Returns the column value for the given column as a long.
     *
     * @param name the column name
     * @return the column value
     */
    long getLong(C name);

    /**
     * Returns the column value for the given column as a long.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    long getLong(String family, C name);

    /**
     * Returns the column value for the given column as a Date.
     *
     * @param name the column name
     * @return the column value
     */
    Date getDate(C name);

    /**
     * Returns the column value for the given column as a Date.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    Date getDate(String family, C name);

    /**
     * Returns the column value for the given column as a float.
     *
     * @param name the column name
     * @return the column value
     */
    float getFloat(C name);

    /**
     * Returns the column value for the given column as a float.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    float getFloat(String family, C name);

    /**
     * Returns the column value for the given column as a double.
     *
     * @param name the column name
     * @return the column value
     */
    double getDouble(C name);

    /**
     * Returns the column value for the given column as a double.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    double getDouble(String family, C name);

    /**
     * Returns the column value for the given column as a byte array.
     *
     * @param name the column name
     * @return the column value
     */
    byte[] getBytes(C name);

    /**
     * Returns the column value for the given column as a byte array.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    byte[] getBytes(String family, C name);

    /**
     * Returns the column value for the given column as a String.
     *
     * @param name the column name
     * @return the column value
     */
    String getString(C name);

    /**
     * Returns the column value for the given column as a String.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    String getString(String family, C name);

    /**
     * Returns the column value for the given column.
     *
     * @param name the column name
     * @return the column value
     */
    <V> V getValue(C name, Codec<V> valueCodec);

    /**
     * Returns the column value for the given column.
     *
     * @param family the column family
     * @param name the column name
     * @return the column value
     */
    <V> V getValue(String family, C name, Codec<V> valueCodec);

    /**
     * Returns the column timestamp for the given column.
     *
     * @param name the column name
     * @return the column timestamp
     */
    long getTimestamp(C name);

    /**
     * Returns the column timestamp for the given column.
     *
     * @param family the column family
     * @param name the column name
     * @return the column timestamp
     */
    long getTimestamp(String family, C name);

    /**
     * Determines whether the column is null.
     *
     * @param name the column name
     * @return true if the column is null; otherwise false
     */
    boolean isNull(C name);

    /**
     * Determines whether the column is null.
     *
     * @param family the column family
     * @param name the column name
     * @return true if the column is null; otherwise false
     */
    boolean isNull(String family, C name);
}
