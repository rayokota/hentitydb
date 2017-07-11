package io.hentitydb.store.hbase;

import com.google.common.base.Throwables;
import io.hentitydb.serialization.Codec;
import io.hentitydb.store.CompareOp;
import io.hentitydb.store.RowMutation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

public class HBaseRowMutations<K, C> extends HBaseRowOperation<K, C> implements io.hentitydb.store.RowMutations<K, C> {

    private final RowMutations mutations;
    private boolean hasPut = false;
    private boolean hasDelete = false;

    public HBaseRowMutations(K key, HBaseTable<K, C> table) {
        super(key, table);
        this.mutations = new RowMutations(table.getMetadata().getKeyCodec().encode(key));
    }

    public RowMutations getHOperation() {
        if (hasPut && hasDelete) {
            // Set ts of puts to be later than deletes
            // See https://issues.apache.org/jira/browse/HBASE-8626
            long now = System.currentTimeMillis();
            for (Mutation mutation : mutations.getMutations()) {
                if (mutation instanceof Delete) {
                    ((Delete)mutation).setTimestamp(now - 10);
                }
            }
        }
        return mutations;
    }

    @Override
    @SafeVarargs
    public final HBaseRowMutations<K, C> add(RowMutation<K, C>... rowMutations) {
        try {
            for (RowMutation<K, C> rowMutation : rowMutations) {
                if (!getKey().equals(rowMutation.getKey())) {
                    throw new IllegalArgumentException("Key does not match");
                }
                if (rowMutation instanceof HBasePut) {
                    hasPut = true;
                    Put put = ((HBasePut) rowMutation).getHOperation();
                    mutations.add(put);
                } else if (rowMutation instanceof HBaseDelete) {
                    hasDelete = true;
                    Delete delete = ((HBaseDelete) rowMutation).getHOperation();
                    mutations.add(delete);
                } else {
                    throw new IllegalArgumentException("Unsupported mutation type " + rowMutation.getClass().getName());
                }
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return this;
    }

    @Override
    public void execute() {
        getTable().doRowMutations(this);
    }

    @Override
    public boolean executeIfAbsent(C column) {
        return executeIfAbsent(getTable().getMetadata().getDefaultFamily(), column);
    }

    @Override
    public boolean executeIfAbsent(String family, C column) {
        return getTable().doRowMutationsIf(family, column, CompareOp.EQUAL, null, this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, boolean value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, boolean value) {
        return getTable().doRowMutationsIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, short value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, short value) {
        return getTable().doRowMutationsIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, int value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, int value) {
        return getTable().doRowMutationsIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, long value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, long value) {
        return getTable().doRowMutationsIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, Date value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, Date value) {
        return getTable().doRowMutationsIf(family, column, compareOp, Bytes.toBytes(value.getTime()), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, float value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, float value) {
        return getTable().doRowMutationsIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, double value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, double value) {
        return getTable().doRowMutationsIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, byte[] value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, byte[] value) {
        return getTable().doRowMutationsIf(family, column, compareOp, value, this);
    }

    @Override
    public boolean executeIf(C column, CompareOp compareOp, String value) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value);
    }

    @Override
    public boolean executeIf(String family, C column, CompareOp compareOp, String value) {
        return getTable().doRowMutationsIf(family, column, compareOp, Bytes.toBytes(value), this);
    }

    @Override
    public <V> boolean executeIf(C column, CompareOp compareOp, V value, Codec<V> valueCodec) {
        return executeIf(getTable().getMetadata().getDefaultFamily(), column, compareOp, value, valueCodec);
    }

    @Override
    public <V> boolean executeIf(String family, C column, CompareOp compareOp, V value, Codec<V> valueCodec) {
        return getTable().doRowMutationsIf(family, column, compareOp, valueCodec.encode(value), this);
    }

    @Override
    public HBaseRowMutations<K, C> setTTL(int ttl) {
        throw new UnsupportedOperationException();
    }
}
