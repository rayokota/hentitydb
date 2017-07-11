package io.hentitydb.store.hbase;

import com.google.common.collect.Lists;
import io.hentitydb.serialization.Codec;
import io.hentitydb.store.Column;
import io.hentitydb.store.Row;
import io.hentitydb.store.TableMetadata;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;
import java.util.List;

public class HBaseRow<K, C> implements Row<K, C> {

    private final String defaultFamily;
    private final Codec<K> keyCodec;
    private final Codec<C> columnCodec;
    private final Result result;

    public HBaseRow(String defaultFamily, Codec<K> keyCodec, Codec<C> columnCodec, Result result) {
        this.keyCodec = keyCodec;
        this.columnCodec = columnCodec;
        this.defaultFamily = defaultFamily;
        this.result = result;
    }

    public HBaseRow(TableMetadata<K, C> tableMetadata, Result result) {
        this(tableMetadata.getDefaultFamily(), tableMetadata.getKeyCodec(), tableMetadata.getColumnCodec(), result);
    }

    public String getDefaultFamily() {
        return defaultFamily;
    }

    public Codec<K> getKeyCodec() {
        return keyCodec;
    }

    public Codec<C> getColumnCodec() {
        return columnCodec;
    }

    @Override
    public K getKey() {
        return getKeyCodec().decode(result.getRow());
    }

    @Override
    public byte[] getRawKey() {
        return result.getRow();
    }

    @Override
    public List<Column<C>> getColumns() {
        List<Column<C>> columns = Lists.newArrayListWithExpectedSize(result.size());
        if (result.listCells() == null) return columns;
        for (Cell cell : result.listCells()) {
            columns.add(new HBaseColumn<>(getColumnCodec(), cell));
        }
        return columns;
    }

    @Override
    public boolean isEmpty() {
        return result.isEmpty();
    }

    @Override
    public int size() {
        return result.size();
    }

    @Override
    public Column<C> getColumn(C name) {
        return getColumn(getDefaultFamily(), name);
    }

    @Override
    public Column<C> getColumn(String family, C name) {
        Cell cell = result.getColumnLatestCell(Bytes.toBytes(family), getColumnCodec().encode(name));
        return cell != null ? new HBaseColumn<>(getColumnCodec(), cell) : null;
    }

    @Override
    public boolean getBoolean(C name) {
        return getBoolean(getDefaultFamily(), name);
    }

    @Override
    public boolean getBoolean(String family, C name) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return Bytes.toBoolean(value);
    }

    @Override
    public short getShort(C name) {
        return getShort(getDefaultFamily(), name);
    }

    @Override
    public short getShort(String family, C name) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return Bytes.toShort(value);
    }

    @Override
    public int getInt(C name) {
        return getInt(getDefaultFamily(), name);
    }

    @Override
    public int getInt(String family, C name) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return Bytes.toInt(value);
    }

    @Override
    public long getLong(C name) {
        return getLong(getDefaultFamily(), name);
    }

    @Override
    public long getLong(String family, C name) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return Bytes.toLong(value);
    }
    @Override
    public Date getDate(C name) {
        return getDate(getDefaultFamily(), name);
    }

    @Override
    public Date getDate(String family, C name) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return new Date(Bytes.toLong(value));
    }

    @Override
    public float getFloat(C name) {
        return getFloat(getDefaultFamily(), name);
    }

    @Override
    public float getFloat(String family, C name) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return Bytes.toFloat(value);
    }

    @Override
    public double getDouble(C name) {
        return getDouble(getDefaultFamily(), name);
    }

    @Override
    public double getDouble(String family, C name) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return Bytes.toDouble(value);
    }

    @Override
    public byte[] getBytes(C name) {
        return getBytes(getDefaultFamily(), name);
    }

    @Override
    public byte[] getBytes(String family, C name) {
        return result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
    }

    @Override
    public String getString(C name) {
        return getString(getDefaultFamily(), name);
    }

    @Override
    public String getString(String family, C name) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return Bytes.toString(value);
    }

    @Override
    public <V> V getValue(C name, Codec<V> valueCodec) {
        return getValue(getDefaultFamily(), name, valueCodec);
    }

    @Override
    public <V> V getValue(String family, C name, Codec<V> valueCodec) {
        byte[] value = result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name));
        return valueCodec.decode(value);
    }

    @Override
    public long getTimestamp(C name) {
        return getTimestamp(getDefaultFamily(), name);
    }

    @Override
    public long getTimestamp(String family, C name) {
        Cell cell = result.getColumnLatestCell(Bytes.toBytes(family), getColumnCodec().encode(name));
        return cell.getTimestamp();
    }

    @Override
    public boolean isNull(C name) {
        return isNull(getDefaultFamily(), name);
    }

    @Override
    public boolean isNull(String family, C name) {
        return result.getValue(Bytes.toBytes(family), getColumnCodec().encode(name)) == null;
    }
}
