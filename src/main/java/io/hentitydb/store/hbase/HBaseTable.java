package io.hentitydb.store.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.hentitydb.serialization.Codec;
import io.hentitydb.serialization.SaltingCodec;
import io.hentitydb.store.*;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An HBase table.
 */
public class HBaseTable<K, C> implements Table<K, C> {

    private final HBaseConnection conn;
    private final TableMetadata<K, C> metadata;
    private final org.apache.hadoop.hbase.client.Table htable;

    /**
     * Creates a new {@link HBaseTable}.
     *
     * @param conn the connection
     * @param metadata the table metadata
     */
    public HBaseTable(HBaseConnection conn,
                      TableMetadata<K, C> metadata) {
        try {
            this.conn = conn;
            this.metadata = metadata;
            this.htable = conn.getHTable(metadata);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

    }

    @Override
    public HBaseConnection getConnection() {
        return conn;
    }

    @Override
    public TableMetadata<K, C> getMetadata() {
        return metadata;
    }

    public TableName getTableName() {
        return metadata.getTableName();
    }

    protected org.apache.hadoop.hbase.client.Table getHTable() {
        return htable;
    }

    /**
     * Creates an executable get operation.
     */
    @Override
    public Get<K, C> get(K key) {
        return new HBaseGet<>(key, this);
    }

    protected Row<K, C> doGet(Get<K, C> get) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            return new HBaseRow<>(getMetadata(), table.get(((HBaseGet<K, C>)get).getHOperation()));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Gets all rows.
     */
    @Override
    @VisibleForTesting
    public RowScanner<K, C> getAll() {
        return new HBaseRowScanner<>(this, new Scan());
    }

    /**
     * Creates an executable increment operation.
     */
    @Override
    public Increment<K, C> increment(K key) {
        return new HBaseIncrement<>(key, this);

    }

    protected void doIncrement(Increment<K, C> increment) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            table.increment(((HBaseIncrement<K, C>)increment).getHOperation());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Creates an executable put operation.
     */
    @Override
    public Put<K, C> put(K key) {
        return new HBasePut<>(key, this);
    }

    protected void doPut(Put<K, C> put) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            table.put(((HBasePut<K, C>) put).getHOperation());
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    protected boolean doPutIf(String family, C column, CompareOp compareOp, byte[] value, Put<K, C> put) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            return table.checkAndPut(HBaseUtil.keyToBytes(put.getKey(), metadata), Bytes.toBytes(family),
                    metadata.getColumnCodec().encode(column), toHOp(compareOp), value, ((HBasePut) put).getHOperation());
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    /**
     * Creates an executable delete operation.
     */
    @Override
    public Delete<K, C> delete(K key) {
        return new HBaseDelete<>(key, this);
    }

    protected void doDelete(Delete<K, C> delete) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            table.delete(((HBaseDelete<K, C>)delete).getHOperation());
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    protected boolean doDeleteIf(String family, C column, CompareOp compareOp, byte[] value, Delete<K, C> delete) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            return table.checkAndDelete(HBaseUtil.keyToBytes(delete.getKey(), metadata), Bytes.toBytes(family),
                    metadata.getColumnCodec().encode(column), toHOp(compareOp), value, ((HBaseDelete) delete).getHOperation());
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    /**
     * Creates an executable batch mutation.
     */
    @Override
    public BatchMutation<K, C> batchMutations() {
        return new HBaseBatchMutation<>(this);
    }

    protected void doBatchMutations(List<RowMutation<K, C>> mutations) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            final Object[] results = new Object[mutations.size()];
            final List<org.apache.hadoop.hbase.client.Row> actions = Lists.newArrayListWithExpectedSize(mutations.size());
            for (RowMutation mutation : mutations) {
                HBaseRowMutation hmutation = (HBaseRowMutation)mutation;
                if (!getTableName().equals(hmutation.getTable().getTableName())) {
                    throw new IllegalArgumentException("Physical tables do not match");
                }
                actions.add(hmutation.getHOperation());
            }
            table.batch(actions, results);
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    /**
     * Creates an executable batch operation.
     */
    @Override
    public BatchOperation<K, C> batchOperations() {
        return new HBaseBatchOperation<>(this);
    }

    protected Object[] doBatchOperations(List<RowOperation<K, C>> operations) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            final Object[] results = new Object[operations.size()];
            final List<org.apache.hadoop.hbase.client.Row> actions = Lists.newArrayListWithExpectedSize(operations.size());
            for (RowOperation operation : operations) {
                HBaseRowOperation hoperation = (HBaseRowOperation)operation;
                if (!getTableName().equals(hoperation.getTable().getTableName())) {
                    throw new IllegalArgumentException("Physical tables do not match");
                }
                actions.add(hoperation.getHOperation());
            }
            table.batch(actions, results);

            final Object[] rows = new Object[results.length];
            for (int i = 0; i < results.length; i++) {
                Object result = results[i];
                rows[i] = result instanceof Result ? new HBaseRow<>(getMetadata(), (Result) result) : result;
            }
            return rows;
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    /**
     * Creates an executable list of atomic mutation operations.
     */
    @Override
    public RowMutations<K, C> mutateRow(K key) {
        return new HBaseRowMutations<>(key, this);
    }

    protected void doRowMutations(RowMutations<K, C> mutations) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            table.mutateRow(((HBaseRowMutations<K, C>)mutations).getHOperation());
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    protected boolean doRowMutationsIf(String family, C column, CompareOp compareOp, byte[] value, RowMutations<K, C> mutations) {
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHTable();
            return table.checkAndMutate(HBaseUtil.keyToBytes(mutations.getKey(), metadata), Bytes.toBytes(family),
                    metadata.getColumnCodec().encode(column), toHOp(compareOp), value, ((HBaseRowMutations<K, C>) mutations).getHOperation());
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    private CompareFilter.CompareOp toHOp(CompareOp compareOp) {
        switch (compareOp) {
            case LESS_THAN:
                return CompareFilter.CompareOp.LESS;
            case GREATER_THAN_EQUAL:
                return CompareFilter.CompareOp.GREATER_OR_EQUAL;
            case EQUAL:
                return CompareFilter.CompareOp.EQUAL;
            case GREATER_THAN:
                return CompareFilter.CompareOp.GREATER;
            case LESS_THAN_EQUAL:
                return CompareFilter.CompareOp.LESS_OR_EQUAL;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Scans over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @return a {@link RowScanner} for the given range
     */
    @Override
    public RowScanner<K, C> scan(K startKey,
                                 K endKey) {
        return scan(startKey, endKey, null, null);
    }

    /**
     * Scans over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param filter      a row filter to apply
     * @return a {@link RowScanner} for the given range
     */
    @Override
    public RowScanner<K, C> scan(K startKey,
                                 K endKey,
                                 Filter<K, C> filter) {
        return scan(startKey, endKey, filter, null);
    }

    /**
     * Scans over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param executor    an executor for parallel scans, if supported
     * @return a {@link RowScanner} for the given range
     */
    public RowScanner<K, C> scan(K startKey,
                                 K endKey,
                                 ExecutorService executor) {
        return scan(startKey, endKey, null, executor);
    }

    /**
     * Scans over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param filter      a row filter to apply
     * @param executor    an executor for parallel scans, if supported
     * @return a {@link RowScanner} for the given range
     */
    public RowScanner<K, C> scan(K startKey,
                                 K endKey,
                                 Filter<K, C> filter,
                                 ExecutorService executor) {
        if (metadata.isSalted()) {
            SaltingCodec<K> saltedCodec = (SaltingCodec<K>) metadata.getKeyCodec();
            Codec<K> unsaltedCodec = saltedCodec.getCodec();
            Scan scan = new Scan(unsaltedCodec.encode(checkNotNull(startKey)),
                    createClosestRowAfter(unsaltedCodec.encode(checkNotNull(endKey))));
            setFilter(scan, filter);
            return new HBaseSaltedRowScanner<>(this, scan, executor);
        } else {
            Scan scan = new Scan(HBaseUtil.keyToBytes(checkNotNull(startKey), metadata),
                    createClosestRowAfter(HBaseUtil.keyToBytes(checkNotNull(endKey), metadata)));
            setFilter(scan, filter);
            return new HBaseRowScanner<>(this, scan);
        }
    }

    /**
     * Scans in reverse over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @return a {@link RowScanner} for the keys in the given range
     */
    @Override
    public RowScanner<K, C> reverseScan(K startKey,
                                        K endKey) {
        return reverseScan(startKey, endKey, null, null);
    }

    /**
     * Scans in reverse over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param filter      a row filter to apply
     * @return a {@link RowScanner} for the keys in the given range
     */
    @Override
    public RowScanner<K, C> reverseScan(K startKey,
                                        K endKey,
                                        Filter<K, C> filter) {
        return reverseScan(startKey, endKey, filter, null);
    }

    /**
     * Scans in reverse over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param executor    an executor for parallel scans, if supported
     * @return a {@link RowScanner} for the given range
     */
    public RowScanner<K, C> reverseScan(K startKey,
                                        K endKey,
                                        ExecutorService executor) {
        return reverseScan(startKey, endKey, null, executor);
    }

    /**
     * Scans in reverse over all entries in the range {@code [startKey, endKey]}.
     *
     * @param startKey    the first key in the range to scan
     * @param endKey      the last key in the range to scan
     * @param executor    an executor for parallel scans, if supported
     * @param filter      a row filter to apply
     * @return a {@link RowScanner} for the given range
     */
    public RowScanner<K, C> reverseScan(K startKey,
                                        K endKey,
                                        Filter<K, C> filter,
                                        ExecutorService executor) {
        if (metadata.isSalted()) {
            SaltingCodec<K> saltedCodec = (SaltingCodec<K>) metadata.getKeyCodec();
            Codec<K> unsaltedCodec = saltedCodec.getCodec();
            Scan scan = new Scan(unsaltedCodec.encode(checkNotNull(startKey)),
                    createClosestRowBefore(unsaltedCodec.encode(checkNotNull(endKey))));
            scan.setReversed(true);
            setFilter(scan, filter);
            return new HBaseSaltedRowScanner<>(this, scan, executor);
        } else {
            Scan scan = new Scan(HBaseUtil.keyToBytes(checkNotNull(startKey), metadata),
                    createClosestRowBefore(HBaseUtil.keyToBytes(checkNotNull(endKey), metadata)));
            scan.setReversed(true);
            setFilter(scan, filter);
            return new HBaseRowScanner<>(this, scan);
        }
    }

    private void setFilter(Scan scan, Filter<K, C> filter) {
        if (filter != null) {
            scan.setFilter(filter instanceof HBaseFilterWrapper ?
                    ((HBaseFilterWrapper) filter).getFilter() :
                    new HBaseFilter<>(filter,
                            getMetadata().getKeyCodec(),
                            getMetadata().getColumnCodec(),
                            true));
        }
    }

    @Override
    @VisibleForTesting
    public void truncate() {
        try {
            BatchMutation<K, C> batchMutation = batchMutations();
            try (RowScanner<K, C> scanner = getAll()) {
                for (Row<K, C> result : scanner) {
                    Delete<K, C> delete = delete(result.getKey());
                    batchMutation.add(delete);
                }
            }
            batchMutation.execute();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    final static byte[] MAX_BYTE_ARRAY = Bytes.createMaxByteArray(9);

    private static byte[] createClosestRowBefore(byte[] row) {
        if (row == null) {
            throw new IllegalArgumentException("The passed row is empty");
        }
        if (Bytes.equals(row, HConstants.EMPTY_BYTE_ARRAY)) {
            return MAX_BYTE_ARRAY;
        }
        if (row[row.length - 1] == 0) {
            return Arrays.copyOf(row, row.length - 1);
        } else {
            byte[] closestFrontRow = Arrays.copyOf(row, row.length);
            closestFrontRow[row.length - 1] = (byte) ((closestFrontRow[row.length - 1] & 0xff) - 1);
            closestFrontRow = Bytes.add(closestFrontRow, MAX_BYTE_ARRAY);
            return closestFrontRow;
        }
    }

    private static byte[] createClosestRowAfter(byte[] row) {
        if (row == null) {
            throw new IllegalArgumentException("The passed row is empty");
        }
        byte[] closestBackRow = Arrays.copyOf(row, row.length + 1);
        closestBackRow[closestBackRow.length - 1] = 0;
        return closestBackRow;
    }

    @Override
    public void close() {
        try {
            htable.close();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public byte[] getRawFilters(List<Filter<K, C>> filters) {
        try {
            List<org.apache.hadoop.hbase.filter.Filter> filterList = Lists.newArrayListWithExpectedSize(filters.size());
            for (Filter<K, C> filter : filters) {
                filterList.add(new HBaseFilter<>(filter,
                        getMetadata().getKeyCodec(),
                        getMetadata().getColumnCodec(),
                        false));
            }
            return new FilterList(filterList).toByteArray();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
