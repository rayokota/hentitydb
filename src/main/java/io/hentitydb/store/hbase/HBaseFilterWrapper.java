package io.hentitydb.store.hbase;

import io.hentitydb.store.AbstractFilter;
import io.hentitydb.store.KeyColumn;
import org.apache.hadoop.hbase.filter.Filter;

public class HBaseFilterWrapper<K, C> extends AbstractFilter<K, C> {

    private Filter filter;

    public HBaseFilterWrapper() {
    }

    public HBaseFilterWrapper(Filter filter) {
        this.filter = filter;
    }

    @Override
    public boolean filterKeyColumn(KeyColumn<K, C> keyColumn) {
        throw new UnsupportedOperationException();
    }

    public Filter getFilter() {
        return filter;
    }
}
