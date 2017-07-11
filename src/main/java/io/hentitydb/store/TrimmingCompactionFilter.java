package io.hentitydb.store;

import com.google.common.collect.Lists;
import io.hentitydb.serialization.Codec;
import io.hentitydb.serialization.ReadBuffer;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class TrimmingCompactionFilter<K, C> implements CompactionFilter<K, C> {

    public static final String MAX_COLUMNS = "maxColumns";
    public static final String MAX_COLUMNS_TTL = "maxColumnsTtl";
    public static final String REFERENCING_FAMILY = "referencingFamily";
    public static final String INDEXING_FAMILY = "indexingFamily";
    public static final String INDEXING_CODECS = "indexingCodecs";

    private int maxCount = Integer.MAX_VALUE;
    private int maxCountTtl = 0;
    private String referencingFamily = null;
    private String indexingFamily = null;
    private List<Codec<?>> firstCodecs = Lists.newArrayList();
    // The last value codec needs to be of the same type as the column codec,
    // but it can be a different codec, such as LongCodec vs VarLongCodec, for example.
    private Codec<C> indexingCodec;

    private int count = 0;
    private int inCount = 0;
    private int outCount = 0;
    private byte[] previousKey = null;

    private final boolean debug = false;

    @Override
    @SuppressWarnings("unchecked")
    public void setup(Map<String, String> config, TableName tableName, String family, String regionName) {
        String maxCountStr = config.get(HENTITYDB_PREFIX + "." + family + "." + MAX_COLUMNS);
        try {
            if (maxCountStr != null) maxCount = Integer.parseInt(maxCountStr);
        } catch (NumberFormatException e) {
            // noop
        }
        String maxCountTtlStr = config.get(HENTITYDB_PREFIX + "." + family + "." + MAX_COLUMNS_TTL);
        try {
            if (maxCountTtlStr != null) maxCountTtl = Integer.parseInt(maxCountTtlStr);
        } catch (NumberFormatException e) {
            // noop
        }
        referencingFamily = config.get(HENTITYDB_PREFIX + "." + family + "." + REFERENCING_FAMILY);
        indexingFamily = config.get(HENTITYDB_PREFIX + "." + family + "." + INDEXING_FAMILY);
        String indexingCodecsStr = config.get(HENTITYDB_PREFIX + "." + family + "." + INDEXING_CODECS);
        if (indexingCodecsStr != null) {
            String[] parts = indexingCodecsStr.split(";");
            for (int i = 0; i < parts.length - 1; i++) {
                firstCodecs.add((Codec<?>)getInstance(parts[i]));
            }
            indexingCodec = (Codec<C>)getInstance(parts[parts.length - 1]);
        }
        if (debug) System.out.println(new Date() + " Started column trimming for: " + tableName + ", " + family + ", " + regionName);
    }

    @Override
    public boolean filterKeyColumn(KeyColumn<K, C> keyColumn) {

        // We ignore timestamps, so different versions of a column will be counted
        if (Arrays.equals(previousKey, keyColumn.getRawKey())) {
            count++;
        } else {
            count = 1;
            previousKey = keyColumn.getRawKey().clone();
        }

        boolean expired = System.currentTimeMillis() - keyColumn.getColumn().getTimestamp() >= maxCountTtl * 1000;
        boolean filter = !expired || count <= maxCount;
        if (filter) inCount++; else outCount++;
        return filter;
    }

    @Override
    public Delete<K, C> deleteAuxiliaryColumns(Delete<K, C> delete, boolean filter, KeyColumn<K, C> keyColumn) {
        if (filter) return null;
        if (referencingFamily == null && (indexingFamily == null || indexingCodec == null)) return null;
        if (referencingFamily != null) {
            delete.addColumn(referencingFamily, keyColumn.getColumn().getName());
        }
        if (indexingFamily != null && indexingCodec != null) {
            byte[] value = keyColumn.getColumn().getBytes();
            ReadBuffer buffer = new ReadBuffer(value);
            for (Codec<?> codec : firstCodecs) {
                codec.decode(buffer);
            }
            C index = indexingCodec.decode(buffer);
            delete.addColumn(indexingFamily, index);
        }
        return delete;
    }

    @Override
    public void close() {
        if (debug) System.out.println(new Date() + " Finished column trimming: in=" + inCount + ", out=" + outCount);
    }

    @SuppressWarnings("unchecked")
    static Object getInstance(String className) {
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
