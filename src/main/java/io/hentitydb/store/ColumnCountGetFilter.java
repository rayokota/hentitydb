package io.hentitydb.store;

import com.google.common.base.Throwables;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;

/**
 * Simple filter than returns the first N columns on a row.
 */
public class ColumnCountGetFilter<K, C> extends AbstractFilter<K, C> {
    private int limit;
    private transient int count = 0;

    // Required for serialization
    public ColumnCountGetFilter() {
    }

    public ColumnCountGetFilter(int limit) {
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    @Override
    public boolean ignoreRemainingRow() {
        return count > limit;
    }

    @Override
    public boolean filterKeyColumn(KeyColumn<K, C> keyColumn) {
        count++;
        return !ignoreRemainingRow();
    }

    @Override
    public void reset() {
        count = 0;
    }

    @Override
    public void encode(Filter<K, C> value, WriteBuffer buffer) {
        buffer.writeVarInt(limit);
    }

    @Override
    public Filter<K, C> decode(ReadBuffer buffer) {
        try {
            int limit = buffer.readVarInt();
            return new ColumnCountGetFilter<>(limit);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
