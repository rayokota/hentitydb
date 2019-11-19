package io.hentitydb.store.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.hentitydb.serialization.Codec;
import io.hentitydb.serialization.SaltingCodec;
import io.hentitydb.store.CompactionFilter;
import io.hentitydb.store.Delete;
import io.hentitydb.store.KeyColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.NoLimitScannerContext;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;


public class HBaseCompactor<K, C> implements RegionObserver {

    private final boolean debug = false;

    @Override
    @SuppressWarnings("unchecked")
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                                      final Store store,
                                      final InternalScanner scanner,
                                      final ScanType scanType,
                                      CompactionLifeCycleTracker tracker,
                                      CompactionRequest request) throws IOException {

        final RegionCoprocessorEnvironment env = e.getEnvironment();
        final Configuration c = env.getConfiguration();
        final Map<String, String> config = getConfig(c);
        final TableName tableName = store.getTableName();
        final String family = store.getColumnFamilyName();
        final String regionName = store.getRegionInfo().getRegionNameAsString();
        final CompactionFilter<K, C> filter = (CompactionFilter<K, C>) getInstance(
                config, CompactionFilter.HENTITYDB_PREFIX + "." + family + "." + CompactionFilter.FILTER);
        if (filter == null) {
            if (debug) System.out.println("WARNING: No filter for: " + tableName + ", " + family);
            return scanner;
        }

        final Codec<K> keyCodec = (Codec<K>) getKeyCodecInstance(config, CompactionFilter.HENTITYDB_PREFIX + "." + CompactionFilter.KEY_CODEC);
        if (keyCodec == null) {
            // we allow the keyCodec to be null; in this case the code will use the raw key
            if (debug) System.out.println("WARNING: No key codec for: " + tableName + ", " + family);
        }

        final Codec<C> columnCodec = (Codec<C>) getInstance(config, CompactionFilter.HENTITYDB_PREFIX + "." + CompactionFilter.COLUMN_CODEC);
        if (columnCodec == null) {
            if (debug) System.out.println("ERROR: No column codec for: " + tableName + ", " + family);
            return scanner;
        }

        try {
            filter.setup(config, new io.hentitydb.store.TableName(tableName.toString()), family, regionName);
        } catch (Exception ex) {
            if (debug) System.out.println("ERROR: Could not set up filter: " + tableName + ", " + family);
            ex.printStackTrace();
            return scanner;
        }

        return new InternalScanner() {

            @Override
            public boolean next(List<Cell> results) throws IOException {
                return next(results, NoLimitScannerContext.getInstance());
            }

            @Override
            public boolean next(List<Cell> results, ScannerContext scannerContext) throws IOException {

                List<Cell> cells = Lists.newArrayList();
                boolean moreRows = scanner.next(cells, scannerContext);

                for (Cell cell : cells) {
                    if (CellUtil.isDelete(cell)) {
                        // pass tombstones through
                        results.add(cell);
                    } else {
                        filterCell(results, cell);
                    }
                }
                return moreRows;
            }

            private void filterCell(List<Cell> results, Cell cell) {
                byte[] key = CellUtil.cloneRow(cell);
                HBaseColumn<C> column = new HBaseColumn<>(columnCodec, cell);
                KeyColumn<K, C> keyColumn = new KeyColumn<>(keyCodec, key, column);

                boolean doFilter = true;
                try {
                    doFilter = filter.filterKeyColumn(keyColumn);
                } catch (Exception ex) {
                    if (debug) System.out.println("ERROR: Could not run filter: " + tableName + ", " + family);
                    ex.printStackTrace();
                }
                if (doFilter) {
                    results.add(cell);
                }

                try {
                    Delete<K, C> delete = keyCodec != null ?
                            new HBaseDelete<>(keyColumn.getKey(), null, keyCodec, columnCodec) :
                            new HBaseDelete<>(key, null, columnCodec);
                    delete = filter.deleteAuxiliaryColumns(delete, doFilter, keyColumn);
                    // don't delete if no columns have been specified as will delete entire row
                    if (delete != null && delete.hasColumns()) {
                        env.getRegion().delete(((HBaseDelete<K, C>) delete).getHOperation());
                    }
                } catch (Exception ex) {
                    if (debug) System.out.println("WARNING: Could not delete auxiliary columns for filter: " + tableName + ", " + family);
                    ex.printStackTrace();
                }
            }

            @Override
            public void close() throws IOException {
                scanner.close();
                if (filter != null) filter.close();
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static Object getKeyCodecInstance(Map<String, String> config, String configKey) {
        String className = config.get(configKey);
        if (className == null) {
            return null;
        }

        try {
            Class cls = Class.forName(className);
            if (cls.equals(SaltingCodec.class)) {
                Codec saltedCodec = (Codec) getInstance(config, CompactionFilter.HENTITYDB_PREFIX + "." + CompactionFilter.SALTED_KEY_CODEC);
                if (saltedCodec == null) return null;
                Constructor ctor = cls.getConstructor(Codec.class);
                return ctor.newInstance(saltedCodec);
            } else {
                return cls.newInstance();
            }
        } catch (Throwable ex) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static Object getInstance(Map<String, String> config, String configKey) {
        String className = config.get(configKey);
        if (className == null) {
            return null;
        }

        try {
            return Class.forName(className).newInstance();
        } catch (Exception ex) {
            return null;
        }
    }

    private Map<String, String> getConfig(Configuration c) {
        Map<String, String> result = Maps.newHashMapWithExpectedSize(c.size());

        for (Map.Entry<String, String> entry : c) {
            if (entry.getKey().startsWith(CompactionFilter.HENTITYDB_PREFIX)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}
