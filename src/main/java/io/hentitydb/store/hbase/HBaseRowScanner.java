package io.hentitydb.store.hbase;

import com.google.common.base.Throwables;
import io.hentitydb.store.Row;
import io.hentitydb.store.RowScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class HBaseRowScanner<K, C> implements RowScanner<K, C> {

    private final HBaseTable<K, C> table;
    private final org.apache.hadoop.hbase.client.ResultScanner scanner;
    private final Iterator<org.apache.hadoop.hbase.client.Result> iterator;


    public HBaseRowScanner(HBaseTable<K, C> table, Scan scan) {
        try {
            this.table = checkNotNull(table);
            this.scanner = table.getHTable().getScanner(scan);
            this.iterator = scanner.iterator();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public Iterator<Row<K, C>> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Row<K, C> next() {
        return new HBaseRow<>(table.getMetadata(), iterator.next());
    }

    @Override
    public void remove() {
        iterator.remove();

    }

    @Override
    public void close() {
        scanner.close();
    }
}
