package io.hentitydb.entity;

import com.google.common.base.Throwables;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;
import io.hentitydb.store.Filter;
import io.hentitydb.store.KeyColumn;
import io.hentitydb.store.TableName;

import java.nio.ByteBuffer;
import java.util.Optional;

public class EntityBoundedFilter<K> extends EntityFilter<K> {

    private int maxEntitiesPerRow = 0;

    private transient int entitiesInRowCount = 0;

    // Required for serialization
    public EntityBoundedFilter() {
    }

    public EntityBoundedFilter(int numComponents, ByteBuffer start, ByteBuffer end,
                               QueryPredicate queryPredicate, int limit, int maxEntitiesPerRow) {
        super(numComponents, start, end, queryPredicate, limit);
        this.maxEntitiesPerRow = maxEntitiesPerRow;
    }

    public EntityBoundedFilter(int numComponents, QueryPredicate queryPredicate,
                               int limit, int maxEntitiesPerRow) {
        super(numComponents, queryPredicate, limit);
        this.maxEntitiesPerRow = maxEntitiesPerRow;
    }

    protected EntityBoundedFilter(int numComponents, ByteBuffer start, ByteBuffer end,
                                  QueryPredicate queryPredicate, int limit, int maxEntitiesPerRow,
                                  TableName tableName) {
        super(numComponents, start, end, queryPredicate, limit, tableName);
        this.maxEntitiesPerRow = maxEntitiesPerRow;
    }

    protected EntityBoundedFilter(int numComponents, QueryPredicate queryPredicate,
                                  int limit, int maxEntitiesPerRow,
                                  TableName tableName) {
        super(numComponents, queryPredicate, limit, tableName);
        this.maxEntitiesPerRow = maxEntitiesPerRow;
    }

    @Override
    public void reset() {
        super.reset();
        entitiesInRowCount = 0;
    }

    @Override
    public boolean filterKeyColumn(KeyColumn<K, byte[]> keyColumn) {
        Optional<Boolean> matchesPrevious = Optional.empty();
        KeyColumn<K, byte[]> previous = getPreviousKeyColumn();
        // We don't check that the keys match as this filter is only used by gets.
        // We don't check that the families match as the maxEntitiesPerRow is only passed
        // when the get is specific to a family
        if (previous != null) {
            matchesPrevious = Optional.of(compare(ByteBuffer.wrap(previous.getColumn().getRawName()),
                    ByteBuffer.wrap(keyColumn.getColumn().getRawName())) == 0);
            if (!matchesPrevious.get()) {
                entitiesInRowCount++;
            }
        }

        int maxEntities = Math.max(maxEntitiesPerRow, getLimit());
        if (maxEntities > 0 && entitiesInRowCount >= maxEntities) {
            setDone(true);
            return false;
        }
        return super.filterKeyColumn(keyColumn, matchesPrevious);
    }

    @Override
    public void encode(Filter<K, byte[]> value, WriteBuffer buffer) {
        buffer.writeVarInt(maxEntitiesPerRow);
        super.encode(value, buffer);
    }

    @Override
    public Filter<K, byte[]> decode(ReadBuffer buffer) {
        try {
            int maxEntitiesPerRow = buffer.readVarInt();
            int numComponents = buffer.readVarInt();
            int limit = buffer.readVarInt();
            ByteBuffer start = BYTE_BUFFER_CODEC.decode(buffer);
            ByteBuffer end = BYTE_BUFFER_CODEC.decode(buffer);
            QueryPredicate queryPredicate = null;
            if (buffer.readByte() == 1) {
                final Class predicateClass = CLASS_CODEC.decode(buffer);
                queryPredicate = (QueryPredicate) predicateClass.newInstance();
                queryPredicate = queryPredicate.decode(buffer);
            }
            return new EntityBoundedFilter<>(numComponents, start, end, queryPredicate, limit, maxEntitiesPerRow);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
