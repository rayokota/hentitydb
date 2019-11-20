package io.hentitydb.store.hbase;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.hentitydb.EntityConfiguration;
import io.hentitydb.serialization.Codec;
import io.hentitydb.serialization.SaltingCodec;
import org.apache.hadoop.hbase.client.mock.MockHTable;
import io.hentitydb.store.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class HBaseConnectionFactory implements ConnectionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnectionFactory.class);

    public static final String HBASE_SECURITY_AUTHENTICATION   = "hbase.security.authentication";
    public static final String HBASE_CLIENT_KERBEROS_PRINCIPAL = "hbase.client.kerberos.principal";
    public static final String HBASE_CLIENT_KEYTAB_FILE        = "hbase.client.keytab.file";
    public static final String HBASE_CLIENT_JAAS_FILE          = "hbase.client.jaas.file";
    public static final String KERBEROS                        = "kerberos";

    private final EntityConfiguration config;
    private final Map<io.hentitydb.store.TableName, TableMetadata<?, ?>> metadata;
    private final Set<io.hentitydb.store.TableName> createdTables;
    private ChoreService choreService = null;

    public HBaseConnectionFactory(EntityConfiguration config) {
        this.config = config;
        this.metadata = Maps.newConcurrentMap();
        this.createdTables = Collections.newSetFromMap(Maps.<io.hentitydb.store.TableName, Boolean>newConcurrentMap());
    }

    @Override
    public EntityConfiguration getConfiguration() {
        return config;
    }

    @Override
    public <K, C> void declareTable(TableMetadata<K, C> metadata) {
        this.metadata.put(metadata.getTableName(), metadata);

        if (!getConfiguration().getTestMode() && getConfiguration().getAutoTableCreation()) {
            try {
                createHTable(metadata);
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Could not create table: " + metadata.getTableName(), e);
            }
        }
    }

    protected org.apache.hadoop.hbase.TableName toHTableName(io.hentitydb.store.TableName tableName) {
        String prefix = getConfiguration().getNamespacePrefix();
        String namespace = prefix != null ? prefix + tableName.getNamespace() : tableName.getNamespace();
        return org.apache.hadoop.hbase.TableName.valueOf(namespace, tableName.getName());
    }

    private <K, C> void createHTable(TableMetadata<K, C> tableMetadata) throws IOException {
        io.hentitydb.store.TableName name = tableMetadata.getTableName();
        if (!createdTables.contains(name)) {
            org.apache.hadoop.hbase.TableName tableName = toHTableName(name);
            try (Admin hadmin = createConnection().getHConnection().getAdmin()) {
                if (!namespaceExists(hadmin, tableName.getNamespaceAsString())) {
                    hadmin.createNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString()).build());
                }
                if (!hadmin.tableExists(tableName)) {
                    Map<String, String> props = Maps.newHashMap();
                    HTableDescriptor desc = createHTableDescriptor(tableMetadata, tableName, props);
                    int numRegions = getConfiguration().getRegionCount();
                    if (numRegions > 1) {
                        hadmin.createTable(desc, getStartKey(numRegions), getEndKey(numRegions), numRegions);
                    } else {
                        hadmin.createTable(desc);
                    }
                }
            }
            createdTables.add(name);
        }
    }

    private static byte[] getStartKey(int regionCount) {
        return Bytes.toBytes((Integer.MAX_VALUE / regionCount));
    }

    private static byte[] getEndKey(int regionCount) {
        return Bytes.toBytes((Integer.MAX_VALUE / regionCount * (regionCount - 1)));
    }

    private boolean namespaceExists(Admin hadmin, String namespace) {
        NamespaceDescriptor namespaceDescriptor = null;
        try {
            namespaceDescriptor = hadmin.getNamespaceDescriptor(namespace);
        } catch (IOException ioe) {
            return false;
        }
        return namespaceDescriptor != null;
    }

    private <K, C> HTableDescriptor createHTableDescriptor(TableMetadata<K, C> tableMetadata,
                                                           org.apache.hadoop.hbase.TableName tableName,
                                                           Map<String, String> props) throws IOException {
        EntityConfiguration config = getConfiguration();
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (ColumnFamilyMetadata<K, C> family : tableMetadata.getColumnFamilies()) {
            HColumnDescriptor c = new HColumnDescriptor(family.getName());
            c.setMaxVersions(1);
            c.setBlocksize(16 * 1024);
            c.setCompressionType(getCompressionType());
            if (family.getTimeToLive() != null) {
                c.setTimeToLive(family.getTimeToLive());
            }
            desc.addFamily(c);
        }
        props = addCompactionProps(tableMetadata, props);
        if (!props.isEmpty()) {
            // add compaction coprocessor
            desc.addCoprocessor(
                    HBaseCompactor.class.getName(),
                    config.getJarFilePath() != null ? new Path(config.getJarFilePath()) : null,
                    Coprocessor.PRIORITY_USER,
                    props);
        }
        return desc;
    }

    public static <K, C> Map<String, String> addCompactionProps(TableMetadata<K, C> tableMetadata,
                                                                Map<String, String> props) {
        for (ColumnFamilyMetadata<K, C> family : tableMetadata.getColumnFamilies()) {
            if (family.getMaxColumns() != null) {
                props.put(CompactionFilter.HENTITYDB_PREFIX + "." + family.getName() + "." + CompactionFilter.FILTER,
                        TrimmingCompactionFilter.class.getName());
                props.put(CompactionFilter.HENTITYDB_PREFIX + "." + family.getName() + "." + TrimmingCompactionFilter.MAX_COLUMNS,
                        String.valueOf(family.getMaxColumns()));
                if (family.getMaxColumnsTtl() != null) {
                    props.put(CompactionFilter.HENTITYDB_PREFIX + "." + family.getName() + "." + TrimmingCompactionFilter.MAX_COLUMNS_TTL,
                            String.valueOf(family.getMaxColumnsTtl()));
                }
                if (family.getReferencingFamily() != null) {
                    props.put(CompactionFilter.HENTITYDB_PREFIX + "." + family.getName() + "." + TrimmingCompactionFilter.REFERENCING_FAMILY,
                            String.valueOf(family.getReferencingFamily()));
                }
                if (family.getIndexingFamily() != null) {
                    props.put(CompactionFilter.HENTITYDB_PREFIX + "." + family.getName() + "." + TrimmingCompactionFilter.INDEXING_FAMILY,
                            String.valueOf(family.getIndexingFamily()));
                    props.put(CompactionFilter.HENTITYDB_PREFIX + "." + family.getName() + "." + TrimmingCompactionFilter.INDEXING_CODECS,
                            codecsToString(family.getIndexingCodecs()));
                }
            } else if (family.getCompactionFilter() != null) {
                props.put(CompactionFilter.HENTITYDB_PREFIX + "." + family.getName() + "." + CompactionFilter.FILTER,
                        family.getCompactionFilter().getName());
                Map<String, String> additionalProps = family.getCompactionFilterProps();
                if (additionalProps != null) props.putAll(additionalProps);
            }
        }
        if (!props.isEmpty()) {
            // add codecs
            Codec<K> keyCodec = tableMetadata.getKeyCodec();
            Codec<C> columnCodec = tableMetadata.getColumnCodec();
            props.put(CompactionFilter.HENTITYDB_PREFIX + "." + CompactionFilter.KEY_CODEC,
                    keyCodec.getClass().getName());
            if (keyCodec instanceof SaltingCodec) {
                Codec<K> saltedKeyCodec = ((SaltingCodec<K>)keyCodec).getCodec();
                props.put(CompactionFilter.HENTITYDB_PREFIX + "." + CompactionFilter.SALTED_KEY_CODEC,
                        saltedKeyCodec.getClass().getName());
            }
            props.put(CompactionFilter.HENTITYDB_PREFIX + "." + CompactionFilter.COLUMN_CODEC,
                    columnCodec.getClass().getName());
        }
        return props;
    }

    private static String codecsToString(List<Codec<?>> codecs) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (Codec<?> codec : codecs) {
            if (!first) sb.append(";"); else first = false;
            sb.append(codec.getClass().getName());
        }
        return sb.toString();
    }

    private Compression.Algorithm getCompressionType() {
        Compression.Algorithm algorithm = Compression.Algorithm.NONE;
        String compression = getConfiguration().getCompression();
        if (compression != null) {
            try {
                algorithm = Compression.Algorithm.valueOf(compression.toUpperCase());
            } catch (IllegalArgumentException e) {
                // ignore
            }
        }
        return algorithm;
    }

    @SuppressWarnings("unchecked")
    @Override
    public TableMetadata<?, ?> getTableMetadata(io.hentitydb.store.TableName tableName) {
        return this.metadata.get(tableName);
    }

    @Override
    public void compactTable(io.hentitydb.store.TableName tableName) {
        try {
            if (!getConfiguration().getTestMode()) {
                try (Admin hadmin = createConnection().getHConnection().getAdmin()) {
                    org.apache.hadoop.hbase.TableName name = toHTableName(tableName);
                    hadmin.flush(name);
                    hadmin.compact(name);
                    hadmin.flush(name);
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void majorCompactTable(io.hentitydb.store.TableName tableName) {
        try {
            if (!getConfiguration().getTestMode()) {
                try (Admin hadmin = createConnection().getHConnection().getAdmin()) {
                    org.apache.hadoop.hbase.TableName name = toHTableName(tableName);
                    hadmin.flush(name);
                    hadmin.majorCompact(name);
                    hadmin.flush(name);
                }
            } else {
                HBaseConnection conn = createConnection();
                HBaseTable<?, ?> table = conn.getTable(tableName);
                ((MockHTable)table.getHTable()).majorCompact(table.getMetadata());
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public boolean isSecure() {
        String auth = config.getHConfiguration().get(HBASE_SECURITY_AUTHENTICATION);
        return KERBEROS.equals(auth);
    }

    @Override
    public HBaseConnection createConnection() {
        return new HBaseConnection(this);
    }

    @Override
    public HBaseConnection createConnection(ExecutorService pool) {
        return new HBaseConnection(this, pool);
    }

    @Override
    public HBaseHealthCheck createHealthCheck(io.hentitydb.store.TableName tableName) {
        return new HBaseHealthCheck(this, tableName);
    }

    public synchronized void scheduleRelogin(UserGroupInformation ugi) {
        if (choreService == null) {
            choreService = new ChoreService("auth");
            choreService.scheduleChore(io.hentitydb.store.hbase.security.AuthUtil.getAuthChore(ugi));
            LOG.debug("Scheduled relogin chore for " + ugi);
        }
    }

    @Override
    public synchronized void close() {
        if (choreService != null && !choreService.isShutdown()) {
            choreService.shutdown();
        }
    }

}
