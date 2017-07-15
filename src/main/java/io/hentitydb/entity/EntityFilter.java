package io.hentitydb.entity;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.hentitydb.serialization.ByteBufferCodec;
import io.hentitydb.serialization.ClassCodec;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;
import io.hentitydb.store.AbstractFilter;
import io.hentitydb.store.Column;
import io.hentitydb.store.Filter;
import io.hentitydb.store.KeyColumn;
import io.hentitydb.store.TableName;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class EntityFilter<K> extends AbstractFilter<K, byte[]> {

    final static ByteBufferCodec BYTE_BUFFER_CODEC = new ByteBufferCodec(true);
    final static ClassCodec CLASS_CODEC = new ClassCodec();

    protected TableName tableName = null;
    protected int limit = 0;
    protected int numComponents;
    protected ByteBuffer start;
    protected ByteBuffer end;
    protected QueryPredicate queryPredicate;

    private transient KeyColumn<K, byte[]> previous = null;
    private transient Map<String, Column<byte[]>> current = Maps.newHashMap();
    private transient int count = 0;
    private transient boolean done = false;

    private final static boolean debug = false;

    final static Maps.EntryTransformer<String, Column<byte[]>, ByteBuffer> COLUMN_TRANSFORMER =
            (key, value) -> value != null ? ByteBuffer.wrap(value.getBytes()) : null;
    final static Maps.EntryTransformer<String, IndexedColumn<byte[]>, ByteBuffer> INDEXED_COLUMN_TRANSFORMER =
            (key, value) -> value != null ? ByteBuffer.wrap(value.getColumn().getBytes()) : null;

    // Required for serialization
    public EntityFilter() {
    }

    public EntityFilter(int numComponents, ByteBuffer start, ByteBuffer end,
                        QueryPredicate queryPredicate, int limit) {
        this.numComponents = numComponents;
        this.start = checkNotNull(start);
        this.end = checkNotNull(end);
        this.queryPredicate = queryPredicate;
        this.limit = limit;
    }

    public EntityFilter(int numComponents, QueryPredicate queryPredicate, int limit) {
        this.numComponents = numComponents;
        this.start = ByteBuffer.allocate(0);
        this.end = ByteBuffer.allocate(0);
        this.queryPredicate = queryPredicate;
        this.limit = limit;
    }

    protected EntityFilter(int numComponents, ByteBuffer start, ByteBuffer end,
                           QueryPredicate queryPredicate, int limit,
                           TableName tableName) {
        this.tableName = tableName;
        this.numComponents = numComponents;
        this.start = checkNotNull(start);
        this.end = checkNotNull(end);
        this.queryPredicate = queryPredicate;
        this.limit = limit;
    }

    protected EntityFilter(int numComponents, QueryPredicate queryPredicate, int limit,
                           TableName tableName) {
        this.tableName = tableName;
        this.numComponents = numComponents;
        this.start = ByteBuffer.allocate(0);
        this.end = ByteBuffer.allocate(0);
        this.queryPredicate = queryPredicate;
        this.limit = limit;
    }

    protected int getLimit() {
        return limit;
    }

    protected int getCount() {
        return count;
    }

    protected int getNumComponents() {
        return numComponents;
    }

    protected KeyColumn<K, byte[]> getPreviousKeyColumn() {
        return previous;
    }

    protected boolean isDone() {
        return done;
    }

    protected void setDone(boolean done) {
        this.done = done;
    }

    @Override
    public void reset() {
        previous = null;
        current.clear();
        count = 0;
        done = false;
    }

    @Override
    public boolean ignoreRemainingRow() {
        return done;
    }

    @Override
    public boolean filterKeyColumn(KeyColumn<K, byte[]> keyColumn) {
        return filterKeyColumn(keyColumn, Optional.empty());
    }

    protected boolean filterKeyColumn(KeyColumn<K, byte[]> keyColumn, Optional<Boolean> matchesPrevious) {
        ByteBuffer columnName = ByteBuffer.wrap(keyColumn.getColumn().getRawName());
        if (start.remaining() != 0 && compare(start, columnName) > 0) {
            return false;
        } else if (end.remaining() != 0 && compare(end, columnName) < 0) {
            // we could have an optimization here to set done = true if
            // all the element IDs are of fixed length (such as Long or Integer)
            // which would allow element IDs to be comparable;
            // however, most use cases use start and limit to get the next page
            return false;
        }
        boolean filter = true;
        //noinspection StatementWithEmptyBody
        if (previous == null ||
                (matchesPrevious.isPresent() && matchesPrevious.get()) ||
                (previous.getColumn().getFamily().equals(keyColumn.getColumn().getFamily()) &&
                        compare(ByteBuffer.wrap(previous.getColumn().getRawName()), columnName) == 0)) {
            // noop
        } else {
            if (checkColumns(current)) {
                count++;
                if (limit > 0 && count >= limit) {
                    done = true;
                    filter = false;
                }
            }
            current.clear();
        }
        String valueName = getValueName(columnName);
        // We may get a duplicate as filterKeyColumn will be called for multiple hfiles
        // In that case keep the more recent one
        // NOTE: this assumes columns are traversed in descending timestamp order
        Column<byte[]> old = current.get(valueName);
        if (old == null) {
            current.put(valueName, keyColumn.getColumn());
        }
        previous = keyColumn;
        return filter;
    }

    @Override
    public boolean hasFilterRow() {
        return queryPredicate != null;
    }

    @Override
    public Set<Integer> filterRow(List<KeyColumn<K, byte[]>> columns) {
        Set<Integer> toKeepIndexes = Sets.newHashSetWithExpectedSize(columns.size());
        // first group columns
        Map<String, IndexedColumn<byte[]>> groupedColumns = Maps.newHashMap();
        KeyColumn<K, byte[]> previous = null;
        int count = 0;
        int index = 0;
        for (KeyColumn<K, byte[]> keyColumn : columns) {
            ByteBuffer columnName = ByteBuffer.wrap(keyColumn.getColumn().getRawName());
            //noinspection StatementWithEmptyBody
            if (previous == null ||
                    (previous.getColumn().getFamily().equals(keyColumn.getColumn().getFamily()) &&
                            compare(ByteBuffer.wrap(previous.getColumn().getRawName()), columnName) == 0)) {
                // noop
            } else {
                if (checkGroupedColumns(groupedColumns)) {
                    count++;
                    for (IndexedColumn<byte[]> indexedColumn : groupedColumns.values()) {
                        toKeepIndexes.add(indexedColumn.getIndex());
                    }
                }
                groupedColumns.clear();
            }
            String valueName = getValueName(columnName);
            IndexedColumn<byte[]> old = groupedColumns.get(valueName);
            if (old == null) {
                groupedColumns.put(valueName, new IndexedColumn<>(index, keyColumn.getColumn()));
            }
            previous = keyColumn;
            index++;
        }
        if (checkGroupedColumns(groupedColumns)) {
            count++;
            for (IndexedColumn<byte[]> indexedColumn : groupedColumns.values()) {
                toKeepIndexes.add(indexedColumn.getIndex());
            }
        }
        if (getLimit() > 0 && getCount() >= getLimit()) {
            if (getCount() != count) {
                if (debug) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("WARNING: ");
                    sb.append(getKeyString(previous));
                    sb.append(" count mismatch, initial = ").append(getCount());
                    sb.append(", final = ").append(count);
                    System.out.println(sb.toString());
                }
            }
        }
        return toKeepIndexes;
    }

    private boolean checkColumns(Map<String, Column<byte[]>> columns) {
        if (columns.isEmpty()) return false;
        return queryPredicate == null || queryPredicate.evaluate(Maps.transformEntries(columns, COLUMN_TRANSFORMER));
    }

    private boolean checkGroupedColumns(Map<String, IndexedColumn<byte[]>> groupedColumns) {
        if (groupedColumns.isEmpty()) return false;
        return queryPredicate == null || queryPredicate.evaluate(Maps.transformEntries(groupedColumns, INDEXED_COLUMN_TRANSFORMER));
    }

    static class IndexedColumn<C> {
        private final int index;
        private final Column<C> column;

        public IndexedColumn(int index, Column<C> column) {
            this.index = index;
            this.column = column;
        }

        public int getIndex() {
            return index;
        }

        public Column<C> getColumn() {
            return column;
        }
    }

    @Override
    public void encode(Filter<K, byte[]> value, WriteBuffer buffer) {
        buffer.writeVarInt(numComponents);
        buffer.writeVarInt(limit);
        BYTE_BUFFER_CODEC.encode(start, buffer);
        BYTE_BUFFER_CODEC.encode(end, buffer);
        buffer.writeByte(queryPredicate != null ? 1 : 0);
        if (queryPredicate != null) {
            CLASS_CODEC.encode(queryPredicate.getClass(), buffer);
            queryPredicate.encode(buffer);
        }
    }

    @Override
    public Filter<K, byte[]> decode(ReadBuffer buffer) {
        try {
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
            return new EntityFilter<>(numComponents, start, end, queryPredicate, limit);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public int compare(ByteBuffer o1, ByteBuffer o2) {
        return EntityMapper.compare(numComponents, o1, o2);
    }

    public String getValueName(ByteBuffer columnName) {
        return EntityMapper.getValueName(numComponents, columnName);
    }

    private String getKeyString(KeyColumn<K, byte[]> keyColumn) {
        String tableString = tableName != null ? tableName.toString() : "";
        String keyString = "N/A";
        if (keyColumn != null) {
            K key = keyColumn.getKey();
            if (key != null) {
                keyString = key.toString();
            }
        }
        return "(" + tableString + ", " + keyString + ")";
    }
}
