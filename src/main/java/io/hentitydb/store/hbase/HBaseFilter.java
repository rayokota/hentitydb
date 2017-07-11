package io.hentitydb.store.hbase;

import com.google.common.collect.Lists;
import io.hentitydb.store.Column;
import io.hentitydb.store.KeyColumn;
import io.hentitydb.util.Iterables;
import io.hentitydb.serialization.ClassCodec;
import io.hentitydb.serialization.Codec;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class HBaseFilter<K, C> extends FilterBase {

    private io.hentitydb.store.Filter<K, C> filter;
    private Codec<K> keyCodec;
    private Codec<C> columnCodec;
    private boolean isRowFilter;

    public HBaseFilter() {
    }

    public HBaseFilter(io.hentitydb.store.Filter<K, C> filter,
                       Codec<K> keyCodec,
                       Codec<C> columnCodec,
                       boolean isRowFilter) {
        this.filter = filter;
        this.keyCodec = keyCodec;
        this.columnCodec = columnCodec;
        this.isRowFilter = isRowFilter;
    }

    @Override
    public void reset() {
        filter.reset();
    }

    @Override
    public Filter.ReturnCode filterKeyValue(Cell cell) {
        boolean doFilter = filter.filterKeyColumn(
                new KeyColumn<>(keyCodec, CellUtil.cloneRow(cell), new HBaseColumn<>(columnCodec, cell)));
        if (isRowFilter) {
            return doFilter ? ReturnCode.INCLUDE : ReturnCode.NEXT_ROW;
        } else {
            return doFilter ? ReturnCode.INCLUDE_AND_NEXT_COL :
                    (filter.ignoreRemainingRow() ? ReturnCode.NEXT_ROW : ReturnCode.NEXT_COL);
        }
    }

    @Override
    public boolean hasFilterRow() {
        return filter.hasFilterRow();
    }

    @Override
    public void filterRowCells(List<Cell> kvs) {
        List<KeyColumn<K, C>> columns = Lists.newArrayListWithExpectedSize(kvs.size());
        for (Cell cell : kvs) {
            Column<C> column = new HBaseColumn<>(columnCodec, cell);
            columns.add(new KeyColumn<>(keyCodec, CellUtil.cloneRow(cell), column));
        }
        final Set<Integer> toKeep = filter.filterRow(columns);
        Iterables.removeIf(kvs, (index, input) -> !toKeep.contains(index));
    }

    @Override
    public Cell transformCell(Cell cell) {
        byte[] newValue = filter.transformKeyColumn(
                new KeyColumn<>(keyCodec, CellUtil.cloneRow(cell), new HBaseColumn<>(columnCodec, cell)));
        if (newValue == null || Arrays.equals(newValue, CellUtil.cloneValue(cell))) {
            return cell;
        } else {
            return CellUtil.createCell(
                    CellUtil.cloneRow(cell),
                    CellUtil.cloneFamily(cell),
                    CellUtil.cloneQualifier(cell),
                    cell.getTimestamp(),
                    cell.getTypeByte(),
                    newValue);
        }
    }

    @Override
    public byte[] toByteArray() throws IOException {
        final WriteBuffer buffer = new WriteBuffer(4096);
        final ClassCodec classCodec = new ClassCodec(HBaseFilter.class.getClassLoader());
        final Codec<Codec<K>> keyCodecCodec = keyCodec.getSelfCodec();
        final Codec<Codec<C>> columnCodecCodec = columnCodec.getSelfCodec();
        classCodec.encode(filter.getClass(), buffer);
        classCodec.encode(keyCodec.getSelfCodec().getClass(), buffer);
        keyCodecCodec.encode(keyCodec, buffer);
        classCodec.encode(columnCodec.getSelfCodec().getClass(), buffer);
        columnCodecCodec.encode(columnCodec, buffer);
        filter.encode(filter, buffer);
        buffer.writeByte(isRowFilter ? 1 : 0);
        return buffer.finish();
    }

    @SuppressWarnings("unchecked")
    public static Filter parseFrom(byte[] pbBytes) throws DeserializationException {
        try {
            final ReadBuffer buffer = new ReadBuffer(pbBytes);
            final ClassCodec classCodec = new ClassCodec(HBaseFilter.class.getClassLoader());
            final Class filterClass = classCodec.decode(buffer);
            final Class keyCodecCodecClass = classCodec.decode(buffer);
            Codec<Codec<?>> keyCodecCodec = (Codec<Codec<?>>)keyCodecCodecClass.newInstance();
            Codec<?> keyCodec = keyCodecCodec.decode(buffer);
            final Class columnCodecCodecClass = classCodec.decode(buffer);
            Codec<Codec<?>> columnCodecCodec = (Codec<Codec<?>>)columnCodecCodecClass.newInstance();
            Codec<?> columnCodec = columnCodecCodec.decode(buffer);
            io.hentitydb.store.Filter<?, ?> filter = (io.hentitydb.store.Filter<?, ?>)filterClass.newInstance();
            filter = filter.decode(buffer);
            boolean isRowFilter = buffer.readByte() == 1;
            return new HBaseFilter(filter, keyCodec, columnCodec, isRowFilter);
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }
}
