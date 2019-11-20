package io.hentitydb.store.hbase;

import io.hentitydb.serialization.Codec;
import io.hentitydb.store.Column;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

public class HBaseColumn<C> implements Column<C> {

    private final Codec<C> columnCodec;
    private final Cell cell;

    public HBaseColumn(Codec<C> columnCodec, Cell cell) {
        this.columnCodec = columnCodec;
        this.cell = cell;
    }

    @Override
    public Codec<C> getColumnCodec() {
        return columnCodec;
    }

    @Override
    public C getName() {
        return columnCodec.decode(CellUtil.cloneQualifier(cell));
    }

    @Override
    public byte[] getRawName() {
        return CellUtil.cloneQualifier(cell);
    }

    @Override
    public String getFamily() {
        return Bytes.toString(CellUtil.cloneFamily(cell));
    }

    @Override
    public boolean getBoolean() {
        return Bytes.toBoolean(CellUtil.cloneValue(cell));
    }

    @Override
    public short getShort() {
        return Bytes.toShort(CellUtil.cloneValue(cell));
    }

    @Override
    public int getInt() {
        return Bytes.toInt(CellUtil.cloneValue(cell));
    }

    @Override
    public long getLong() {
        return Bytes.toLong(CellUtil.cloneValue(cell));
    }

    @Override
    public Date getDate() {
        return new Date(Bytes.toLong(CellUtil.cloneValue(cell)));
    }

    @Override
    public float getFloat() {
        return Bytes.toFloat(CellUtil.cloneValue(cell));
    }

    @Override
    public double getDouble() {
        return Bytes.toDouble(CellUtil.cloneValue(cell));
    }

    @Override
    public byte[] getBytes() {
        return CellUtil.cloneValue(cell);
    }

    @Override
    public String getString() {
        return Bytes.toString(CellUtil.cloneValue(cell));
    }

    @Override
    public <V> V getValue(Codec<V> valueCodec) {
        byte[] value = CellUtil.cloneValue(cell);
        return value != null && value.length > 0 ? valueCodec.decode(value) : null;
    }

    @Override
    public long getTimestamp() {
        return cell.getTimestamp();
    }
}
