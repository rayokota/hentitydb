package io.hentitydb.store.hbase;

import com.google.common.collect.Lists;
import io.hentitydb.serialization.Codec;
import io.hentitydb.store.BooleanOp;
import io.hentitydb.store.Row;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class HBaseGet<K, C> extends HBaseRowOperation<K, C> implements io.hentitydb.store.Get<K, C> {

    private final String defaultFamily;
    private final Codec<K> keyCodec;
    private final Codec<C> columnCodec;
    private final Get get;
    private final List<Filter> filterList;
    private BooleanOp filterOp;

    public HBaseGet(K key, String defaultFamily, Codec<K> keyCodec, Codec<C> columnCodec) {
        super(key, null);
        this.defaultFamily = defaultFamily;
        this.keyCodec = keyCodec;
        this.columnCodec = columnCodec;
        this.get = new Get(HBaseUtil.keyToBytes(key, keyCodec));
        this.filterList = Lists.newArrayList();
        this.filterOp = BooleanOp.AND;
    }

    public HBaseGet(K key, HBaseTable<K, C> table) {
        super(key, table);
        this.defaultFamily = table.getMetadata().getDefaultFamily();
        this.keyCodec = table.getMetadata().getKeyCodec();
        this.columnCodec = table.getMetadata().getColumnCodec();
        this.get = new Get(HBaseUtil.keyToBytes(key, keyCodec));
        this.filterList = Lists.newArrayList();
        this.filterOp = BooleanOp.AND;
    }

    public HBaseGet(byte[] rawKey, String defaultFamily, Codec<C> columnCodec) {
        super(null, null);
        this.defaultFamily = defaultFamily;
        this.keyCodec = null;
        this.columnCodec = columnCodec;
        this.get = new Get(rawKey);
        this.filterList = Lists.newArrayList();
        this.filterOp = BooleanOp.AND;
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
    public Get getHOperation() {
        if (!filterList.isEmpty()) get.setFilter(new FilterList(getFilterListOperator(filterOp), filterList));
        return get;
    }

    private static FilterList.Operator getFilterListOperator(BooleanOp filterOp) {
        switch (filterOp) {
            case AND:
                return FilterList.Operator.MUST_PASS_ALL;
            case OR:
                return FilterList.Operator.MUST_PASS_ONE;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public HBaseGet<K, C> addAll() {
        return this;
    }

    @Override
    public HBaseGet<K, C> addColumn(C column) {
        return addColumn(getDefaultFamily(), column);
    }

    @Override
    public HBaseGet<K, C> addColumn(String family, C column) {
        get.addColumn(Bytes.toBytes(family), getColumnCodec().encode(column));
        return this;
    }

    @Override
    public HBaseGet<K, C> addFamily(String family) {
        get.addFamily(Bytes.toBytes(family));
        return this;
    }

    @Override
    public HBaseGet<K, C> withColumnLimit(int limit) {
        get.setMaxResultsPerColumnFamily(limit);
        return this;
    }

    @Override
    public HBaseGet<K, C> withColumnRange(C startColumn, C endColumn) {
        filterList.add(new ColumnRangeFilter(
                getColumnCodec().encode(startColumn), true,
                getColumnCodec().encode(endColumn), true));
        return this;
    }

    @Override
    public HBaseGet<K, C> withColumnRange(C startColumn, C endColumn, int limit) {
        filterList.add(new ColumnRangeFilter(
                getColumnCodec().encode(startColumn), true,
                getColumnCodec().encode(endColumn), true));
        get.setMaxResultsPerColumnFamily(limit);
        return this;
    }

    @Override
    public HBaseGet<K, C> withColumnRange(C startColumn,
                                          boolean startColumnInclusive,
                                          C endColumn,
                                          boolean endColumnInclusive,
                                          int limit) {
        filterList.add(new ColumnRangeFilter(
                getColumnCodec().encode(startColumn), startColumnInclusive,
                getColumnCodec().encode(endColumn), endColumnInclusive));
        get.setMaxResultsPerColumnFamily(limit);
        return this;
    }

    @Override
    public HBaseGet<K, C> withColumnRange(C startColumn,
                                          boolean startColumnInclusive,
                                          C endColumn,
                                          boolean endColumnInclusive,
                                          int limit,
                                          io.hentitydb.store.Filter<K, C>... filters) {
        List<Filter> hfilters = Lists.newArrayList();
        hfilters.add(new ColumnRangeFilter(
                getColumnCodec().encode(startColumn), startColumnInclusive,
                getColumnCodec().encode(endColumn), endColumnInclusive));
        for (io.hentitydb.store.Filter<K, C> filter : filters) {
            hfilters.add(new HBaseFilter<>(filter, getKeyCodec(), getColumnCodec(), false));
        }
        filterList.add(new FilterList(hfilters));
        get.setMaxResultsPerColumnFamily(limit);
        return this;
    }

    @Override
    public HBaseGet<K, C> addFilter(io.hentitydb.store.Filter<K, C> filter) {
        filterList.add(filter instanceof HBaseFilterWrapper ?
                ((HBaseFilterWrapper) filter).getFilter() :
                new HBaseFilter<>(filter,
                        getKeyCodec(),
                        getColumnCodec(),
                        false));
        return this;
    }

    @Override
    public HBaseGet<K, C> setFilterOp(BooleanOp op) {
        this.filterOp = op;
        return this;
    }

    @Override
    public Row<K, C> execute() {
        return getTable().doGet(this);
    }
}
