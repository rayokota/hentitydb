package io.hentitydb.store.hbase;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.hentitydb.store.Row;
import io.hentitydb.store.RowScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;

public class HBaseSaltedRowScanner<K, C> implements RowScanner<K, C> {

    private final HBaseTable<K, C> table;
    private final ListeningExecutorService executorService;
    private final List<ResultScanner> scanners;
    private final List<Iterator<org.apache.hadoop.hbase.client.Result>> iterators;
    private final Iterator<org.apache.hadoop.hbase.client.Result> mergedIterator;


    public HBaseSaltedRowScanner(HBaseTable<K, C> table, final Scan scan, ExecutorService executor) {
        try {
            this.table = checkNotNull(table);
            this.executorService = executor != null ? MoreExecutors.listeningDecorator(executor) : null;
            this.scanners = executor != null ?
                    Collections.synchronizedList(Lists.<ResultScanner>newArrayList()) :
                    Lists.newArrayList();
            this.iterators = computeIterators(scan);
            mergedIterator = Iterators.mergeSorted(iterators, (o1, o2) -> scan.isReversed() ?
                Bytes.compareTo(o2.getRow(), o1.getRow()) :
                Bytes.compareTo(o1.getRow(), o2.getRow()));
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    private List<Iterator<org.apache.hadoop.hbase.client.Result>> computeIterators(Scan scan) throws Exception {
        final List<Iterator<org.apache.hadoop.hbase.client.Result>> iterators;
        Scan[] scans = getDistributedScans(scan);
        if (executorService != null) {
            List<ListenableFuture<Iterator<Result>>> futures = Lists.newArrayList();
            for (final Scan partialScan : scans) {
                futures.add(executorService.submit(() -> {
                    ResultScanner scanner = table.getHTable().getScanner(partialScan);
                    scanners.add(scanner);
                    return scanner.iterator();
                }));
            }
            ListenableFuture<List<Iterator<Result>>> allAsList = Futures.allAsList(futures);
            return allAsList.get();
        } else {
            iterators = Lists.newArrayList();
            for (Scan partialScan : scans) {
                ResultScanner scanner = table.getHTable().getScanner(partialScan);
                scanners.add(scanner);
                iterators.add(scanner.iterator());
            }
            return iterators;
        }
    }

    @Override
    public Iterator<Row<K, C>> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return mergedIterator.hasNext();
    }

    @Override
    public Row<K, C> next() {
        return new HBaseRow<>(table.getMetadata(), mergedIterator.next());
    }

    @Override
    public void remove() {
        mergedIterator.remove();
    }

    @Override
    public void close() {
        for (ResultScanner scanner : scanners) {
            scanner.close();
        }
    }

    /**
     * Gets all distributed intervals based on the original start & stop keys.
     * Used when scanning all buckets based on start/stop row keys. Should return keys so that all buckets in which
     * records between originalStartKey and originalStopKey were distributed are "covered".
     *
     * From http://blog.sematext.com/2012/04/09/hbasewd-avoid-regionserver-hotspotting-despite-writing-records-with-sequential-keys/
     *
     * @param originalStartKey start key
     * @param originalStopKey stop key
     * @return array[Pair(startKey, stopKey)]
     */
    @SuppressWarnings("unchecked")
    public Pair<byte[], byte[]>[] getDistributedIntervals(byte[] originalStartKey, byte[] originalStopKey) {
        byte[][] startKeys = getAllDistributedKeys(originalStartKey);
        byte[][] stopKeys;
        if (Arrays.equals(originalStopKey, HConstants.EMPTY_END_ROW)) {
            Arrays.sort(startKeys, Bytes.BYTES_RAWCOMPARATOR);
            // stop keys are the start key of the next interval
            stopKeys = getAllDistributedKeys(HConstants.EMPTY_BYTE_ARRAY);
            for (int i = 0; i < stopKeys.length - 1; i++) {
                stopKeys[i] = stopKeys[i + 1];
            }
            stopKeys[stopKeys.length - 1] = HConstants.EMPTY_END_ROW;
        } else {
            stopKeys = getAllDistributedKeys(originalStopKey);
            assert stopKeys.length == startKeys.length;
        }

        Pair<byte[], byte[]>[] intervals = new Pair[startKeys.length];
        for (int i = 0; i < startKeys.length; i++) {
            intervals[i] = new Pair<>(startKeys[i], stopKeys[i]);
        }

        return intervals;
    }

    public final Scan[] getDistributedScans(Scan original) throws IOException {
        Pair<byte[], byte[]>[] intervals = getDistributedIntervals(original.getStartRow(), original.getStopRow());

        Scan[] scans = new Scan[intervals.length];
        for (int i = 0; i < intervals.length; i++) {
            scans[i] = new Scan(original);
            scans[i].setStartRow(intervals[i].getFirst());
            scans[i].setStopRow(intervals[i].getSecond());
        }
        return scans;
    }

    public byte[][] getAllDistributedKeys(byte[] originalKey) {
        byte[][] allPrefixes = table.getMetadata().getAllSaltingPrefixes();
        byte[][] keys = new byte[allPrefixes.length][];
        for (int i = 0; i < allPrefixes.length; i++) {
            keys[i] = Bytes.add(allPrefixes[i], originalKey);
        }

        return keys;
    }
}
