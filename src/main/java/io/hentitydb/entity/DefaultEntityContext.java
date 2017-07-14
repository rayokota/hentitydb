package io.hentitydb.entity;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.hentitydb.serialization.ByteArrayCodec;
import io.hentitydb.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.PersistenceException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DefaultEntityContext<T, K> implements EntityContext<T, K> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultEntityContext.class);

    private final Connection connection;
    private final EntityMapper<T, K> entityMapper;

    public DefaultEntityContext(Connection connection, Class<T> entityType) {
        Preconditions.checkNotNull(connection, "Missing withConnection() clause");
        Preconditions.checkNotNull(entityType, "Missing withEntityType() clause");

        this.connection = connection;
        this.entityMapper = new EntityMapper<>(entityType);

        List<ColumnFamilyMetadata<K, byte[]>> columnFamilies = entityMapper.getColumnFamilies();
        ColumnFamilyMetadata<K, byte[]> defaultFamily = entityMapper.getDefaultColumnFamily();
        TableMetadata<K, byte[]> tableMetadata = new TableMetadata<>(
                entityMapper.getTableName(),
                defaultFamily,
                MappingUtils.getCodecForField(this.entityMapper.getId(), false),
                new ByteArrayCodec(false));
        boolean skip = true;
        for (ColumnFamilyMetadata<K, byte[]> columnFamily : columnFamilies) {
            if (skip) {
                // skip first (default) family
                skip = false;
                continue;
            }
            tableMetadata.addColumnFamily(columnFamily);
        }

        connection.getConnectionFactory().declareTable(tableMetadata);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table<K, byte[]> getTable() {
        return connection.getTable(entityMapper.getTableName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(T entity) throws PersistenceException {
        try (Table<K, byte[]> table = getTable()) {
            Put<K, byte[]> put = rawPut(table, entity);
            put.execute();
        } catch (Exception e) {
            throw new PersistenceException("Failed to put entity ", e);
        }
    }

    private Put<K, byte[]> rawPut(Table<K, byte[]> table, T entity) throws PersistenceException {
        return entityMapper.fillMutationBatch(table, entity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(Collection<T> entities) throws PersistenceException {
        try (Table<K, byte[]> table = getTable()) {
            Map<K, Collection<T>> entitiesById = Maps.newLinkedHashMap();
            for (T entity : entities) {
                K id = entityMapper.getEntityId(entity);
                Collection<T> children = entitiesById.computeIfAbsent(id, k -> Lists.newArrayList());
                children.add(entity);
            }
            BatchMutation<K, byte[]> batchMutation = entityMapper.fillMutationBatch(table, entitiesById);
            batchMutation.execute();
        } catch (Exception e) {
            throw new PersistenceException("Failed to put entity ", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> getAll() throws PersistenceException {
        try (Table<K, byte[]> table = getTable()) {
            List<Row<K, byte[]>> rows = Lists.newArrayList();
            RowScanner<K, byte[]> scanner = table.getAll();
            for (Row<K, byte[]> row : scanner) {
                rows.add(row);
            }
            return convertRowsToEntities(rows);
        } catch (Exception e) {
            throw new PersistenceException("Failed to get all entities");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> get(K id) throws PersistenceException {
        try {
            SelectQuery<T, K> query = select().whereId().eq(id);
            return query.fetch();
        } catch (Exception e) {
            throw new PersistenceException("Failed to get entity " + id, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(K id) throws PersistenceException {
        try (Table<K, byte[]> table = getTable()) {
            Delete<K, byte[]> clm = table.delete(id);
            clm.execute();
        } catch (Exception e) {
            throw new PersistenceException("Failed to delete entity " + id, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(T entity) throws PersistenceException {
        K id = null;
        try (Table<K, byte[]> table = getTable()) {
            id = entityMapper.getEntityId(entity);
            Delete<K, byte[]> delete = rawRemove(table, entity);
            delete.execute();
        } catch (Exception e) {
            throw new PersistenceException("Failed to delete entity " + id, e);
        }
    }

    protected Delete<K, byte[]> rawRemove(Table<K, byte[]> table, T entity) throws PersistenceException {
        return entityMapper.fillMutationBatchForDelete(table, entity);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(Collection<T> entities) throws PersistenceException {
        try (Table<K, byte[]> table = getTable()) {
            Map<K, Collection<T>> entitiesById = Maps.newLinkedHashMap();
            for (T entity : entities) {
                K id = entityMapper.getEntityId(entity);
                Collection<T> children = entitiesById.computeIfAbsent(id, k -> Lists.newArrayList());
                children.add(entity);
            }
            BatchMutation<K, byte[]> batchMutation = entityMapper.fillMutationBatchForDelete(table, entitiesById);
            batchMutation.execute();
        } catch (Exception e) {
            throw new PersistenceException("Failed to put entity ", e);
        }
    }

    /**
     * Converts the given rows to entities.
     *
     * @param rows the rows
     * @return the entities
     */
    protected List<T> convertRowsToEntities(List<Row<K, byte[]>> rows) {
        List<T> entities = Lists.newArrayList();
        for (Row<K, byte[]> row : rows) {
            List<Column<byte[]>> cl = row.getColumns();
            if (!cl.isEmpty()) {
                // first group columns
                List<List<Column<byte[]>>> columnLists = Lists.newArrayList();
                List<Column<byte[]>> columns = Lists.newArrayList();
                Column<byte[]> previous = null;
                for (Column<byte[]> column : cl) {
                    //noinspection StatementWithEmptyBody
                    if (previous == null ||
                            (previous.getFamily().equals(column.getFamily()) &&
                             entityMapper.compare(
                                ByteBuffer.wrap(previous.getRawName()),
                                ByteBuffer.wrap(column.getRawName())) == 0)) {
                        // noop
                    } else {
                        columnLists.add(columns);
                        columns = Lists.newArrayList();
                    }
                    columns.add(column);
                    previous = column;
                }
                columnLists.add(columns);
                for (List<Column<byte[]>> columnList : columnLists) {
                    T entity = entityMapper.constructEntity(row.getKey(), columnList);
                    entities.add(entity);
                }
            }
        }
        return entities;
    }

    /**
     * Converts the given rows to a count.
     *
     * @param rows the rows
     * @return the count of entities
     */
    protected int convertRowsToCount(List<Row<K, byte[]>> rows) {
        int size = 0;
        for (Row<K, byte[]> row : rows) {
            List<Column<byte[]>> cl = row.getColumns();
            if (!cl.isEmpty()) {
                Column<byte[]> previous = null;
                for (Column<byte[]> column : cl) {
                    if (previous == null
                        || !(previous.getFamily().equals(column.getFamily()))
                        || entityMapper.compare(
                                ByteBuffer.wrap(previous.getRawName()),
                                ByteBuffer.wrap(column.getRawName())) != 0) {
                        size++;
                    }
                    previous = column;
                }
            }
        }
        return size;
    }

    /**
     * Converts the given column to fields.
     *
     * @param columnName the raw column name
     * @param value the raw column value
     * @return the fields
     */
    protected Map<String, Object> convertColumnToFields(ByteBuffer columnName, ByteBuffer value) {
        return entityMapper.getEntityFieldsFromColumnName(columnName, value);
    }

    @Override
    public SelectQuery<T, K> select() {
        return new SelectQuery<T, K>() {
            @Override
            public T fetchOne() throws PersistenceException {
                return Iterables.getFirst(fetch(), null);
            }

            @SuppressWarnings("unchecked")
            @Override
            public List<T> fetch() throws PersistenceException {
                try (Table<K, byte[]> table = getTable()) {
                    ByteBuffer[] endpoints = getEndpoints();
                    Get<K, byte[]> rowQuery = prepareQuery(table, family, id, endpoints, columnPredicate);
                    Row<K, byte[]> row = rowQuery.execute();
                    List<Row<K, byte[]>> rows = Lists.newArrayList(row);

                    return convertRowsToEntities(rows);
                } catch (Exception e) {
                    throw new PersistenceException("Error executing select query", e);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public int count() {
                try (Table<K, byte[]> table = getTable()) {
                    ByteBuffer[] endpoints = getEndpoints();
                    Get<K, byte[]> rowQuery = prepareQuery(table, family, id, endpoints, columnPredicate);
                    Row<K, byte[]> row = rowQuery.execute();
                    List<Row<K, byte[]>> rows = Lists.newArrayList(row);

                    return convertRowsToCount(rows);
                } catch (Exception e) {
                    throw new PersistenceException("Error executing count query", e);
                }
            }

            private Get<K, byte[]> prepareQuery(Table<K, byte[]> table,
                                                String family,
                                                K id,
                                                ByteBuffer[] endpoints,
                                                BooleanPredicate columnPredicate) {
                TableName tableName = table.getMetadata().getTableName();
                Get<K, byte[]> get = table.get(id);
                Filter<K, byte[]> filter = null;
                if (family != null) {
                    get = get.addFamily(family);
                    Integer maxEntitiesPerRow = entityMapper.getMaxEntitiesPerRow(family);
                    if (maxEntitiesPerRow != null) {
                        filter = getBoundedFilter(tableName, endpoints, columnPredicate, maxEntitiesPerRow);
                    }
                }
                if (filter == null) {
                    filter = getFilter(tableName, endpoints, columnPredicate);
                }
                return get.addFilter(filter);
            }

            private ByteBuffer[] getEndpoints() {
                ByteBuffer[] endpoints = null;
                if (elementIdPredicates != null && !elementIdPredicates.isEmpty()) {
                    endpoints = entityMapper.getQueryEndpoints(elementIdPredicates);
                }
                return endpoints;
            }

            private Filter<K, byte[]> getFilter(TableName tableName,
                                                ByteBuffer[] endpoints,
                                                BooleanPredicate columnPredicate) {
                if (endpoints != null) {
                    return new EntityFilter<>(
                            entityMapper.getNumComponents(),
                            endpoints[0], endpoints[1], columnPredicate, entityLimit);
                } else {
                    return new EntityFilter<>(
                            entityMapper.getNumComponents(),
                            columnPredicate, entityLimit);
                }
            }

            private Filter<K, byte[]> getBoundedFilter(TableName tableName,
                                                       ByteBuffer[] endpoints,
                                                       BooleanPredicate columnPredicate,
                                                       int maxEntitiesPerRow) {
                if (endpoints != null) {
                    return new EntityBoundedFilter<>(
                            entityMapper.getNumComponents(),
                            endpoints[0], endpoints[1],
                            columnPredicate, entityLimit, maxEntitiesPerRow);
                } else {
                    return new EntityBoundedFilter<>(
                            entityMapper.getNumComponents(),
                            columnPredicate, entityLimit, maxEntitiesPerRow);
                }
            }

            @Override
            protected ByteBuffer getRawValue(String name, Object value, boolean isComponent) {
                return getRawValueUsingMapper(name, value, isComponent);
            }
        };
    }

    @Override
    public SelectOneQuery<T, K> selectOne() {
        return new SelectOneQuery<T, K>() {
            @Override
            public T fetchOne() throws PersistenceException {
                return Iterables.getFirst(fetch(), null);
            }

            @SuppressWarnings("unchecked")
            public List<T> fetch() throws PersistenceException {
                try (Table<K, byte[]> table = getTable()) {
                    Get<K, byte[]> entityQuery = entityMapper.fillGet(table, family, id, elementIds);
                    Row<K, byte[]> entity = entityQuery.execute();
                    List<Row<K, byte[]>> rows = Lists.newArrayList(entity);

                    return convertRowsToEntities(rows);
                } catch (Exception e) {
                    throw new PersistenceException("Error executing select query", e);
                }
            }
        };
    }

    @Override
    public UpdateQuery<T, K> update() {
        return new UpdateQuery<T, K>() {
            @Override
            public boolean execute() {
                try (Table<K, byte[]> table = getTable()) {
                    Put<K, byte[]> put = rawPut(table);
                    if (!ifEqualsElementIds.isEmpty()) {
                        if (ifColumnPredicate != null) {
                            String family = ifFamily != null ? ifFamily : entityMapper.getDefaultColumnFamily().getName();
                            ByteBuffer columnName = entityMapper.toColumnName(ifEqualsElementIds, ifColumnPredicate.getName());
                            ByteBuffer value = ifColumnPredicate.getValue();
                            byte[] columnNameBytes = EntityMapper.BYTE_BUFFER_CODEC.encode(columnName);
                            return value != null
                                    ? put.executeIf(family, columnNameBytes, ifColumnPredicate.getOp().reverse(),
                                        EntityMapper.BYTE_BUFFER_CODEC.encode(value))
                                    : put.executeIfAbsent(family, columnNameBytes);
                        } else {
                            throw new IllegalArgumentException("Missing ifEquals() clause");
                        }
                    } else {
                        put.execute();
                        return true;
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new PersistenceException("Error executing update query", e);
                }
            }

            @Override
            protected Put<K, byte[]> rawPut(Table<K, byte[]> table) {
                return entityMapper.fillMutationBatch(table, family, id, elementIds, setColumns);
            }

            @Override
            protected ByteBuffer getRawValue(String name, Object value, boolean isComponent) {
                return getRawValueUsingMapper(name, value, isComponent);
            }
        };
    }

    @Override
    public DeleteQuery<T, K> delete() {
        return new DeleteQuery<T, K>() {
            @Override
            public boolean execute() {
                try (Table<K, byte[]> table = getTable()) {
                    Delete<K, byte[]> delete = rawRemove(table);
                    if (!ifEqualsElementIds.isEmpty()) {
                        if (ifColumnPredicate != null) {
                            String family = ifFamily != null ? ifFamily : entityMapper.getDefaultColumnFamily().getName();
                            ByteBuffer columnName = entityMapper.toColumnName(ifEqualsElementIds, ifColumnPredicate.getName());
                            ByteBuffer value = ifColumnPredicate.getValue();
                            byte[] columnNameBytes = EntityMapper.BYTE_BUFFER_CODEC.encode(columnName);
                            return value != null
                                    ? delete.executeIf(family, columnNameBytes, ifColumnPredicate.getOp().reverse(),
                                        EntityMapper.BYTE_BUFFER_CODEC.encode(value))
                                    : delete.executeIfAbsent(family, columnNameBytes);
                        } else {
                            throw new IllegalArgumentException("Missing ifEquals() clause");
                        }
                    } else {
                        delete.execute();
                        return true;
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new PersistenceException("Error executing delete query", e);
                }
            }

            @Override
            protected Delete<K, byte[]> rawRemove(Table<K, byte[]> table) {
                return entityMapper.fillMutationBatchForDelete(table, family, id, elementIds, columns);
            }

            @Override
            protected ByteBuffer getRawValue(String name, Object value, boolean isComponent) {
                return getRawValueUsingMapper(name, value, isComponent);
            }
        };
    }

    @Override
    public MutationsQuery<T, K> mutate() {
        return new MutationsQuery<T, K>() {
            @SuppressWarnings("unchecked")
            @Override
            public boolean execute() {
                try (Table<K, byte[]> table = getTable()) {
                    RowMutations<K, byte[]> rowMutations = table.mutateRow(id);
                    for (MutationQuery<T, K> mutationQuery : mutations) {
                        if (mutationQuery instanceof UpdateQuery) {
                            UpdateQuery<T, K> updateQuery = (UpdateQuery<T, K>) mutationQuery;
                            if (!updateQuery.ifEqualsElementIds.isEmpty()) {
                                throw new PersistenceException("Query in mutations query cannot have if-predicates");
                            }
                            rowMutations.add(updateQuery.rawPut(table));
                        } else if (mutationQuery instanceof DeleteQuery) {
                            DeleteQuery<T, K> deleteQuery = (DeleteQuery<T, K>) mutationQuery;
                            if (!deleteQuery.ifEqualsElementIds.isEmpty()) {
                                throw new PersistenceException("Query in mutations query cannot have if-predicates");
                            }
                            rowMutations.add(deleteQuery.rawRemove(table));
                        } else {
                            throw new IllegalArgumentException();
                        }
                    }
                    if (!ifEqualsElementIds.isEmpty()) {
                        if (ifColumnPredicate != null) {
                            String family = ifFamily != null ? ifFamily : entityMapper.getDefaultColumnFamily().getName();
                            ByteBuffer columnName = entityMapper.toColumnName(ifEqualsElementIds, ifColumnPredicate.getName());
                            ByteBuffer value = ifColumnPredicate.getValue();
                            byte[] columnNameBytes = EntityMapper.BYTE_BUFFER_CODEC.encode(columnName);
                            return value != null
                                    ? rowMutations.executeIf(family, columnNameBytes, ifColumnPredicate.getOp().reverse(),
                                    EntityMapper.BYTE_BUFFER_CODEC.encode(value))
                                    : rowMutations.executeIfAbsent(family, columnNameBytes);
                        } else {
                            throw new IllegalArgumentException("Missing ifEquals() clause");
                        }
                    } else {
                        rowMutations.execute();
                        return true;
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new PersistenceException("Error executing mutations query", e);
                }
            }

            @Override
            protected ByteBuffer getRawValue(String name, Object value, boolean isComponent) {
                return getRawValueUsingMapper(name, value, isComponent);
            }
        };
    }

    private ByteBuffer getRawValueUsingMapper(String name, Object value, boolean isComponent) {
        FieldMapper<?> valueMapper = isComponent ? entityMapper.getComponentMapper(name) : entityMapper.getValueMapper(name);
        if (valueMapper == null) {
            throw new IllegalArgumentException("Field '" + name + "' is not a valid column");
        }
        return valueMapper.valueToByteBuffer(value);
    }

    @Override
    public void truncate() throws PersistenceException {
        try (Table<K, byte[]> table = getTable()) {
            table.truncate();
        } catch (Exception e) {
            throw new PersistenceException("Unable to drop table " + entityMapper.getTableName(), e);
        }
    }
}
