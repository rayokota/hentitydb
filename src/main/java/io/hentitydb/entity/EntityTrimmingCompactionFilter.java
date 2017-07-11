package io.hentitydb.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.hentitydb.serialization.Codec;
import io.hentitydb.serialization.LongCodec;
import io.hentitydb.store.CompactionFilter;
import io.hentitydb.store.Delete;
import io.hentitydb.store.KeyColumn;
import io.hentitydb.store.TableName;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class EntityTrimmingCompactionFilter<K> implements CompactionFilter<K, byte[]> {

    public static final String MAX_ENTITIES = "maxEntities";
    public static final String MAX_ENTITIES_TTL = "maxEntitiesTtl";
    public static final String NUM_ELEMENT_IDS = "numElementIds";
    public static final String REFERENCING_FAMILY = "referencingFamily";
    public static final String INDEXING_FAMILY = "indexingFamily";
    public static final String INDEXING_COLUMN_CODEC = "indexingColumnCodec";
    public static final String INDEXING_VALUE_CODEC = "indexingValueCodec";
    public static final String INDEXING_VALUE_NAME = "indexingValueName";
    public static final String VALUE_NAMES = "valueNames";

    private int maxCount = Integer.MAX_VALUE;
    private int maxCountTtl = 0;
    private int numElementIds = 0;
    private String referencingFamily;
    private String indexingFamily;
    // Currently only works with long codecs
    private Codec<Long> indexingColumnCodec;
    private Codec<Long> indexingValueCodec;
    private String indexingValueName;
    private List<String> valueNames = Lists.newArrayList();

    private int count = 0;
    private KeyColumn<K, byte[]> previous = null;
    private int inCount = 0;
    private int outCount = 0;

    private final boolean debug = false;

    @Override
    @SuppressWarnings("unchecked")
    public void setup(Map<String, String> config, TableName tableName, String family, String regionName) {
        String maxCountStr = config.get(HENTITYDB_PREFIX + "." + family + "." + MAX_ENTITIES);
        try {
            if (maxCountStr != null) maxCount = Integer.parseInt(maxCountStr);
        } catch (NumberFormatException e) {
            // noop
        }
        String maxCountTtlStr = config.get(HENTITYDB_PREFIX + "." + family + "." + MAX_ENTITIES_TTL);
        try {
            if (maxCountTtlStr != null) maxCountTtl = Integer.parseInt(maxCountTtlStr);
        } catch (NumberFormatException e) {
            // noop
        }

        String numElementIdsStr = config.get(HENTITYDB_PREFIX + "." + family + "." + NUM_ELEMENT_IDS);
        try {
            if (numElementIdsStr != null) numElementIds = Integer.parseInt(numElementIdsStr);
        } catch (NumberFormatException e) {
            // noop
        }
        referencingFamily = config.get(HENTITYDB_PREFIX + "." + family + "." + REFERENCING_FAMILY);
        indexingFamily = config.get(HENTITYDB_PREFIX + "." + family + "." + INDEXING_FAMILY);
        indexingColumnCodec = (Codec<Long>) getInstance(config, CompactionFilter.HENTITYDB_PREFIX + "." + family + "." + INDEXING_COLUMN_CODEC);
        if (indexingColumnCodec == null) {
            if (debug) System.out.println("WARNING: No column codec for: " + tableName + ", " + family);
            indexingColumnCodec = new LongCodec();
        }
        indexingValueCodec = (Codec<Long>) getInstance(config, CompactionFilter.HENTITYDB_PREFIX + "." + family + "." + INDEXING_VALUE_CODEC);
        if (indexingValueCodec == null) {
            if (debug) System.out.println("WARNING: No value codec for: " + tableName + ", " + family);
            indexingValueCodec = new LongCodec();
        }
        indexingValueName = config.get(HENTITYDB_PREFIX + "." + family + "." + INDEXING_VALUE_NAME);
        String valueNamesStr = config.get(HENTITYDB_PREFIX + "." + family + "." + VALUE_NAMES);
        if (valueNamesStr != null) {
            String[] parts = valueNamesStr.split(";");
            for (String part : parts) {
                valueNames.add(part);
            }
        }
        if (debug) System.out.println(new Date() + " Started entity trimming for: " + tableName + ", " + family + ", " + regionName);
    }

    @Override
    public boolean filterKeyColumn(KeyColumn<K, byte[]> keyColumn) {

        // We don't check that the families match as compaction is specific to a family
        if (previous != null && Arrays.equals(previous.getRawKey(), keyColumn.getRawKey())) {
            if (EntityMapper.compare(numElementIds,
                    ByteBuffer.wrap(previous.getColumn().getRawName()),
                    ByteBuffer.wrap(keyColumn.getColumn().getRawName())) != 0) {
                count++;
            }
        } else {
            count = 1;
        }
        previous = keyColumn;

        boolean expired = System.currentTimeMillis() - keyColumn.getColumn().getTimestamp() >= maxCountTtl * 1000;
        boolean filter = !expired || count <= maxCount;
        if (filter) inCount++; else outCount++;
        return filter;
    }

    @Override
    public Delete<K, byte[]> deleteAuxiliaryColumns(Delete<K, byte[]> delete, boolean filter, KeyColumn<K, byte[]> keyColumn) {
        if (filter) return null;
        if (referencingFamily == null && (indexingFamily == null || indexingValueName == null)) return null;
        if (referencingFamily != null) {
            ByteBuffer componentBytes = ByteBuffer.wrap(keyColumn.getColumn().getRawName());
            if (EntityMapper.isEntityMarker(numElementIds, componentBytes)) {
                // delete entity marker
                deleteColumn(delete, referencingFamily, numElementIds, componentBytes, null);
                // delete columns of the referencing family
                for (String valueName : valueNames) {
                    deleteColumn(delete, referencingFamily, numElementIds, componentBytes, valueName);
                }
            }
        }
        if (indexingFamily != null && indexingValueName != null) {
            if (indexingValueName.equals(EntityMapper.getValueName(
                    numElementIds, ByteBuffer.wrap(keyColumn.getColumn().getRawName())))) {
                Long value = keyColumn.getColumn().getValue(indexingValueCodec);
                if (value != null) { // may be a tombstone
                    List<ByteBuffer> byteBuffers = ImmutableList.of(ByteBuffer.wrap(indexingColumnCodec.encode(value)));
                    // using the long index, delete entity marker
                    deleteColumn(delete, indexingFamily, byteBuffers, null);
                    // using the long index, delete columns of the indexing family
                    for (String valueName : valueNames) {
                        deleteColumn(delete, indexingFamily, byteBuffers, valueName);
                    }
                }
            }
        }
        return delete;
    }

    private void deleteColumn(Delete<K, byte[]> delete, String family, int numComponents, ByteBuffer componentBytes, String valueName) {
        ByteBuffer columnNameBytes = EntityMapper.toColumnName(
                numComponents, componentBytes, valueName);
        byte[] bytes = new byte[columnNameBytes.remaining()];
        columnNameBytes.get(bytes);
        delete.addColumn(family, bytes);
    }

    private void deleteColumn(Delete<K, byte[]> delete, String family, List<ByteBuffer> byteBuffers , String valueName) {
        ByteBuffer columnNameBytes = EntityMapper.toColumnName(
                byteBuffers, valueName);
        byte[] bytes = new byte[columnNameBytes.remaining()];
        columnNameBytes.get(bytes);
        delete.addColumn(family, bytes);
    }

    @Override
    public void close() {
        if (debug) System.out.println(new Date() + " Finished entity trimming: in=" + inCount + ", out=" + outCount);
    }

    @SuppressWarnings("unchecked")
    private static Object getInstance(Map<String, String> config, String configKey) {
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
}
