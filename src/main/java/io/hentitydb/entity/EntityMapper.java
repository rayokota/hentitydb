package io.hentitydb.entity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.hentitydb.serialization.ByteBufferCodec;
import io.hentitydb.serialization.BytesUtil;
import io.hentitydb.serialization.LongCodec;
import io.hentitydb.serialization.StringCodec;
import io.hentitydb.store.*;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.PersistenceException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EntityMapper<T, K> {

    final static String DEFAULT_CF_NAME = "c";
    final static ByteBufferCodec BYTE_BUFFER_CODEC = new ByteBufferCodec(false);
    final static StringCodec STRING_CODEC = new StringCodec(false);

    /**
     * Entity class.
     */
    private final Class<T> clazz;

    /**
     * Entity name.
     */
    private final String entityName;

    /**
     * Table name.
     */
    private final TableName tableName;

    /**
     * Maximum entities per row for each column family.
     */
    private Map<String, Integer> maxEntitiesPerRow = Maps.newHashMap();

    /**
     * Default column family.
     */
    private List<ColumnFamilyMetadata<K, byte[]>> columnFamilies = Lists.newArrayList();

    /**
     * TTL supplier method
     */
    private final Method ttlMethod;

    /**
     * ID field (same as row key).
     */
    private final FieldMapper<K> idMapper;

    /**
     * Column family name field (optional)
     */
    private final FieldMapper<K> cfNameMapper;

    /**
     * List of serializers for the composite parts.
     */
    private final List<FieldMapper<?>> components = Lists.newArrayList();

    /**
     * List of valid (i.e. existing) component names.
     */
    private final Set<String> componentNames = Sets.newHashSet();

    /**
     * Mapper for the value parts of the entity.
     */
    private final Map<String, FieldMapper<?>> valueMappers = Maps.newHashMap();

    /**
     * Largest buffer size.
     */
    private final static int BUFFER_SIZE = 64;

    @SuppressWarnings("unchecked")
    public EntityMapper(Class<T> clazz) {
        this.clazz = clazz;

        Entity entityAnnotation = clazz.getAnnotation(Entity.class);
        if (entityAnnotation == null) {
            throw new IllegalArgumentException("Missing @Entity annotation: " + clazz.getName());
        }
        entityName = MappingUtils.getEntityName(entityAnnotation, clazz);

        javax.persistence.Table tableAnnotation = clazz.getAnnotation(javax.persistence.Table.class);
        tableName = new TableName(tableAnnotation != null ? tableAnnotation.name() : entityName);

        Field[] declaredFields = clazz.getDeclaredFields();
        FieldMapper<K> tempIdMapper = null;
        FieldMapper<K> tempCfNameMapper = null;
        for (Field field : declaredFields) {
            // Should only have one id field and it should map to the row key
            Id idAnnotation = field.getAnnotation(Id.class);
            if (idAnnotation != null) {
                Preconditions.checkArgument(tempIdMapper == null, "Already specified @Id annotation");
                field.setAccessible(true);
                tempIdMapper = new FieldMapper<>(field);
            }

            ElementId compositeAnnotation = field.getAnnotation(ElementId.class);
            if (compositeAnnotation != null) {
                field.setAccessible(true);
                FieldMapper<?> fieldMapper = new FieldMapper(field);
                components.add(fieldMapper);
                componentNames.add(fieldMapper.getName());
            } else {
                javax.persistence.Column columnAnnotation = field.getAnnotation(javax.persistence.Column.class);
                if (columnAnnotation != null) {
                    field.setAccessible(true);
                    FieldMapper<?> fieldMapper = new FieldMapper(field);
                    valueMappers.put(fieldMapper.getName(), fieldMapper);
                }
            }

            ColumnFamilyName cfNameAnnotation = field.getAnnotation(ColumnFamilyName.class);
            if (cfNameAnnotation != null) {
                Preconditions.checkArgument(tempCfNameMapper == null, "Already specified @ColumnFamilyName annotation");
                field.setAccessible(true);
                tempCfNameMapper = new FieldMapper<>(field);
            }
        }

        ColumnFamilies cfAnnotation = clazz.getAnnotation(ColumnFamilies.class);
        if (cfAnnotation != null) {
            ColumnFamily[] families = cfAnnotation.value();
            for (ColumnFamily family : families) {
                String familyName = family.name();
                Class filterClass = null;
                Map<String, String> filterProps = null;
                int familyMaxEntitiesPerRow = family.maxEntitiesPerRow();
                if (familyMaxEntitiesPerRow > 0) {
                    maxEntitiesPerRow.put(familyName, familyMaxEntitiesPerRow);
                    filterClass = EntityTrimmingCompactionFilter.class;
                    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
                            .put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.MAX_ENTITIES,
                                String.valueOf(familyMaxEntitiesPerRow))
                            .put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.NUM_ELEMENT_IDS,
                                String.valueOf(getNumComponents()));
                    if (family.maxEntitiesPerRowTtl() > 0) {
                        builder.put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.MAX_ENTITIES_TTL,
                            String.valueOf(family.maxEntitiesPerRowTtl()));
                    }
                    if (!family.referencingFamily().isEmpty()) {
                        builder.put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.REFERENCING_FAMILY,
                                String.valueOf(family.referencingFamily()));
                    }
                    if (!family.indexingFamily().isEmpty()) {
                        FieldMapper<?> indexedColumn = getIndexedColumn();
                        builder.put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.INDEXING_FAMILY,
                                String.valueOf(family.indexingFamily()))
                                .put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.INDEXING_COLUMN_CODEC,
                                        indexedColumn.getCodec().getClass().getName())
                                .put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.INDEXING_VALUE_NAME,
                                        String.valueOf(family.indexingValueName()))
                                .put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.INDEXING_VALUE_CODEC,
                                        LongCodec.class.getName());
                    }
                    if (!family.referencingFamily().isEmpty() || !family.indexingFamily().isEmpty()) {
                        builder.put(CompactionFilter.HENTITYDB_PREFIX + "." + familyName + "." + EntityTrimmingCompactionFilter.VALUE_NAMES,
                                getValueNamesAsString());
                    }
                    filterProps = builder.build();
                }
                columnFamilies.add(new ColumnFamilyMetadata<>(
                        familyName,
                        family.ttl() > 0 ? family.ttl() : null,
                        filterClass,
                        filterProps));
            }
        } else {
            columnFamilies.add(new ColumnFamilyMetadata<>(DEFAULT_CF_NAME, null, null));
        }

        // TTL method
        Method tmpTtlMethod = null;
        for (Method method : this.clazz.getDeclaredMethods()) {
            if (method.isAnnotationPresent(TTL.class)) {
                Preconditions.checkState(tmpTtlMethod == null, "Duplicate TTL method annotation on " + method.getName());
                tmpTtlMethod = method;
                tmpTtlMethod.setAccessible(true);
            }
        }
        this.ttlMethod = tmpTtlMethod;

        Preconditions.checkNotNull(tempIdMapper, "Missing @Id annotation");
        idMapper = tempIdMapper;
        cfNameMapper = tempCfNameMapper;
    }

    private FieldMapper<?> getIndexedColumn() {
        if (componentNames.size() != 1) throw new IllegalStateException("Indexes only supported for single element ID");
        FieldMapper<?> fieldMapper = components.get(0);
        Field field = fieldMapper.getField();
        Class<?> type = field.getType();
        if (type != Long.class && type != long.class) throw new IllegalStateException("Indexes only supported for element ID of type Long or long");
        return fieldMapper;
    }

    TableName getTableName() {
        return tableName;
    }

    List<ColumnFamilyMetadata<K, byte[]>> getColumnFamilies() {
        return columnFamilies;
    }

    Integer getMaxEntitiesPerRow(String family) {
        return maxEntitiesPerRow.get(family);
    }

    int getNumComponents() {
        return components.size();
    }

    int getNumValueMappers() {
        return valueMappers.size();
    }

    FieldMapper<?> getComponentMapper(String name) {
        for (FieldMapper<?> mapper : components) {
            if (mapper.getName().equals(name)) {
                return mapper;
            }
        }
        return null;
    }

    Set<String> getValueNames() {
        return valueMappers.keySet();
    }

    String getValueNamesAsString() {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String valueName : getValueNames()){
            if (!first) sb.append(";"); else first = false;
            sb.append(valueName);
        }
        return sb.toString();
    }

    FieldMapper<?> getValueMapper(String name) {
        return valueMappers.get(name);
    }

    Get<K, byte[]> fillGet(Table<K, byte[]> table,
                           String columnFamily,
                           K id,
                           Map<String, Object> elementIds) {
        try {
            String family = columnFamily != null ? columnFamily : getDefaultColumnFamily().getName();
            Get<K, byte[]> get = table.get(id);
            ByteBuffer columnName = toColumnName(elementIds, null);
            get.addColumn(family, BYTE_BUFFER_CODEC.encode(columnName));
            for (String valueName : getValueNames()) {
                ByteBuffer name = toColumnName(elementIds, valueName);
                get.addColumn(family, BYTE_BUFFER_CODEC.encode(name));
            }
            return get;
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill get", e);
        }
    }

    Put<K, byte[]> fillMutationBatch(Table<K, byte[]> table, T entity) {
        try {
            Put<K, byte[]> put = table.put(getEntityId(entity));
            return fillMutationBatch(put, entity);
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill mutation batch", e);
        }
    }

    Put<K, byte[]> fillMutationBatch(Put<K, byte[]> put, T entity) {
        try {
            Integer ttl = getTtl(entity);
            if (ttl != null) put.setTTL(ttl);
            ByteBuffer columnName = toColumnName(entity, null);
            put.addColumn(getColumnFamilyName(entity), BYTE_BUFFER_CODEC.encode(columnName), new byte[0]);
            for (FieldMapper<?> valueMapper : valueMappers.values()) {
                ByteBuffer name = toColumnName(entity, valueMapper.getName());
                ByteBuffer value = valueMapper.toByteBuffer(entity);
                if (value != null) {
                    put.addColumn(getColumnFamilyName(entity), BYTE_BUFFER_CODEC.encode(name), BYTE_BUFFER_CODEC.encode(value));
                } else {
                    put.addColumn(getColumnFamilyName(entity), BYTE_BUFFER_CODEC.encode(name), new byte[0]);
                }
            }
            return put;
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill mutation batch", e);
        }
    }

    Put<K, byte[]> fillMutationBatch(Table<K, byte[]> table,
                                     String columnFamily,
                                     K id,
                                     Map<String, Object> elementIds,
                                     Map<String, Object> setColumns) {
        try {
            String family = columnFamily != null ? columnFamily : getDefaultColumnFamily().getName();
            Put<K, byte[]> put = table.put(id);
            ByteBuffer columnName = toColumnName(elementIds, null);
            put.addColumn(family, BYTE_BUFFER_CODEC.encode(columnName), new byte[0]);
            for (Map.Entry<String, Object> setColumn : setColumns.entrySet()) {
                ByteBuffer name = toColumnName(elementIds, setColumn.getKey());
                ByteBuffer value = valueMappers.get(setColumn.getKey()).valueToByteBuffer(setColumn.getValue());
                if (value != null) {
                    put.addColumn(family, BYTE_BUFFER_CODEC.encode(name), BYTE_BUFFER_CODEC.encode(value));
                } else {
                    put.addColumn(family, BYTE_BUFFER_CODEC.encode(name), new byte[0]);
                }
            }
            return put;
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill mutation batch", e);
        }
    }

    BatchMutation<K, byte[]> fillMutationBatch(Table<K, byte[]> table, Map<K, Collection<T>> entitiesById) {
        try {
            BatchMutation<K, byte[]> batchMutation = table.batchMutations();
            for (Map.Entry<K, Collection<T>> entities : entitiesById.entrySet()) {
                Put<K, byte[]> put = table.put(entities.getKey());
                for (T entity : entities.getValue()) {
                    put = fillMutationBatch(put, entity);
                }
                batchMutation.add(put);
            }
            return batchMutation;
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill mutation batch", e);
        }
    }

    Delete<K, byte[]> fillMutationBatchForDelete(Table<K, byte[]> table, T entity) {
        try {
            Delete<K, byte[]> delete = table.delete(getEntityId(entity));
            return fillMutationBatchForDelete(delete, entity);
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill mutation batch", e);
        }
    }

    Delete<K, byte[]> fillMutationBatchForDelete(Delete<K, byte[]> delete, T entity) {
        try {
            ByteBuffer columnName = toColumnName(entity, null);
            delete.addColumn(getColumnFamilyName(entity), BYTE_BUFFER_CODEC.encode(columnName));
            for (FieldMapper<?> valueMapper : valueMappers.values()) {
                ByteBuffer name = toColumnName(entity, valueMapper.getName());
                delete.addColumn(getColumnFamilyName(entity), BYTE_BUFFER_CODEC.encode(name));
            }
            return delete;
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill mutation batch", e);
        }
    }

    Delete<K, byte[]> fillMutationBatchForDelete(Table<K, byte[]> table,
                                                 String columnFamily,
                                                 K id,
                                                 Map<String, Object> elementIds,
                                                 List<String> columns) {
        try {
            String family = columnFamily != null ? columnFamily : getDefaultColumnFamily().getName();
            Delete<K, byte[]> delete = table.delete(id);
            if (elementIds.size() > 0) {
                if (columns.size() > 0) {
                    for (String column : columns) {
                        ByteBuffer name = toColumnName(elementIds, column);
                        delete.addColumn(family, BYTE_BUFFER_CODEC.encode(name));
                    }
                } else {
                    // Delete all columns for the entity
                    ByteBuffer columnName = toColumnName(elementIds, null);
                    delete.addColumn(family, BYTE_BUFFER_CODEC.encode(columnName));
                    for (FieldMapper<?> valueMapper : valueMappers.values()) {
                        ByteBuffer name = toColumnName(elementIds, valueMapper.getName());
                        delete.addColumn(family, BYTE_BUFFER_CODEC.encode(name));
                    }
                }
            }
            return delete;
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill mutation batch", e);
        }
    }

    BatchMutation<K, byte[]> fillMutationBatchForDelete(Table<K, byte[]> table, Map<K, Collection<T>> entitiesById) {
        try {
            BatchMutation<K, byte[]> batchMutation = table.batchMutations();
            for (Map.Entry<K, Collection<T>> entities : entitiesById.entrySet()) {
                Delete<K, byte[]> delete = table.delete(entities.getKey());
                for (T entity : entities.getValue()) {
                    delete = fillMutationBatchForDelete(delete, entity);
                }
                batchMutation.add(delete);
            }
            return batchMutation;
        } catch (Exception e) {
            throw new PersistenceException("Failed to fill mutation batch", e);
        }
    }

    private Integer getTtl(T entity) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Integer retTtl = null;
        if (ttlMethod != null) {
            Object retobj = ttlMethod.invoke(entity);
            retTtl = (Integer) retobj;
        }
        return retTtl;
    }

    protected ByteBuffer toColumnName(Object obj, String valueName) {
        CompositeBuilder composite = new CompositeBuilder(BUFFER_SIZE, CompareOp.EQUAL);

        // Iterate through each component and add to a CompositeType structure
        try {
            for (FieldMapper<?> mapper : components) {
                composite.addWithoutControl(mapper.toByteBuffer(obj));
            }
            if (valueName != null) {
                composite.addWithoutControl(ByteBuffer.wrap(STRING_CODEC.encode(valueName)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return composite.get();
    }

    protected ByteBuffer toColumnName(Map<String, Object> obj, String valueName) {
        CompositeBuilder composite = new CompositeBuilder(BUFFER_SIZE, CompareOp.EQUAL);

        // Iterate through each component and add to a CompositeType structure
        try {
            for (FieldMapper<?> mapper : components) {
                if (obj.get(mapper.getName()) == null) {
                    throw new IllegalArgumentException("Field '" + mapper.getName() + "' is not a valid element ID");
                }
                composite.addWithoutControl(mapper.toByteBuffer(obj));
            }
            if (valueName != null) {
                composite.addWithoutControl(ByteBuffer.wrap(STRING_CODEC.encode(valueName)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return composite.get();
    }

    // Extract the component bytes from the byte buffer and add the value name
    public static ByteBuffer toColumnName(int numComponents, ByteBuffer byteBuffer, String valueName) {
        CompositeBuilder composite = new CompositeBuilder(BUFFER_SIZE, CompareOp.EQUAL);

        ByteBuffer bb = byteBuffer.duplicate();
        int i = 0;
        while (bb.remaining() > 0 && i < numComponents) {
            int length = getShortLength(bb);
            composite.addWithoutControl(getBytes(bb, length));

            // consume equality
            byte b = bb.get();
            i++;
        }
        if (valueName != null) {
            // ignore the value name in bb, using the parameter instead
            composite.addWithoutControl(ByteBuffer.wrap(STRING_CODEC.encode(valueName)));
        }
        return composite.get();
    }

    // Write the raw byte buffers
    public static ByteBuffer toColumnName(List<ByteBuffer> byteBuffers, String valueName) {
        CompositeBuilder composite = new CompositeBuilder(BUFFER_SIZE, CompareOp.EQUAL);

        for (ByteBuffer bb : byteBuffers) {
            composite.addWithoutControl(bb.duplicate());
        }
        if (valueName != null) {
            composite.addWithoutControl(ByteBuffer.wrap(STRING_CODEC.encode(valueName)));
        }
        return composite.get();
    }

    T constructEntity(K id, List<Column<byte[]>> columns) {
        try {
            // First, construct the parent class and give it an id
            T entity = clazz.newInstance();
            setEntityId(entity, id);
            if (!columns.isEmpty()) {
                setColumnFamilyName(entity, columns.get(0).getFamily());
            }
            for (Column<byte[]> column : columns) {
                setEntityFieldsFromColumnName(entity,
                        ByteBuffer.wrap(column.getRawName()),
                        ByteBuffer.wrap(column.getBytes()));
            }
            return entity;
        } catch (Exception e) {
            throw new PersistenceException("Failed to construct entity", e);
        }
    }

    @SuppressWarnings("unchecked")
    public K getEntityId(T entity) throws IllegalAccessException {
        return idMapper.getValue(entity);
    }

    public void setEntityId(T entity, K id) throws IllegalAccessException {
        idMapper.setValue(entity, id);
    }

    @VisibleForTesting
    Field getId() {
        return idMapper.field;
    }

    @SuppressWarnings("unchecked")
    public String getColumnFamilyName(T entity) throws IllegalAccessException {
        String family = null;
        if (cfNameMapper != null) {
            family = (String) cfNameMapper.getValue(entity);
        }
        if (family == null) {
            family = getDefaultColumnFamily().getName();
        }
        return family;
    }

    public void setColumnFamilyName(T entity, String family) throws IllegalAccessException {
        if (cfNameMapper != null) cfNameMapper.setValue(entity, family);
    }

    public String getEntityName() {
        return entityName;
    }

    public ColumnFamilyMetadata<K, byte[]> getDefaultColumnFamily() {
        return columnFamilies.get(0);
    }

    @Override
    public String toString() {
        return String.format("EntityMapper(%s)", clazz);
    }

    Map<String, Object> getEntityFieldsFromColumnName(ByteBuffer columnName, ByteBuffer columnValue) {
        Map<String, Object> result = Maps.newHashMap();
        // Iterate through components in order and set fields
        for (FieldMapper<?> component : components) {
            ByteBuffer data = getWithShortLength(columnName);
            if (data != null) {
                if (data.remaining() > 0) {
                    result.put(component.getName(), component.fromByteBuffer(data));
                }
                byte end_of_component = columnName.get();
                if (end_of_component != CompareOp.EQUAL.toByte()) {
                    throw new RuntimeException("Invalid composite column.  Expected END_OF_COMPONENT.");
                }
            } else {
                throw new RuntimeException("Missing component data in composite type");
            }
        }

        if (columnName.remaining() > 0) {
            ByteBuffer data = getWithShortLength(columnName);
            if (data != null && data.remaining() > 0) {
                byte[] bytes = BYTE_BUFFER_CODEC.encode(data);
                String valueName = STRING_CODEC.decode(bytes);
                FieldMapper<?> valueMapper = valueMappers.get(valueName);
                result.put(valueName, valueMapper.fromByteBuffer(columnValue));
                byte end_of_component = columnName.get();
                if (end_of_component != CompareOp.EQUAL.toByte()) {
                    throw new RuntimeException("Invalid composite column.  Expected END_OF_COMPONENT.");
                }
            } else {
                throw new RuntimeException("Invalid value name in composite type");
            }
        }
        return result;
    }

    void setEntityFieldsFromColumnName(Object entity, ByteBuffer columnName, ByteBuffer columnValue)
            throws IllegalArgumentException, IllegalAccessException {
        // Iterate through components in order and set fields
        for (FieldMapper<?> component : components) {
            ByteBuffer data = getWithShortLength(columnName);
            if (data != null) {
                if (data.remaining() > 0) {
                    component.setField(entity, data);
                }
                byte end_of_component = columnName.get();
                if (end_of_component != CompareOp.EQUAL.toByte()) {
                    throw new RuntimeException("Invalid composite column.  Expected END_OF_COMPONENT.");
                }
            } else {
                throw new RuntimeException("Missing component data in composite type");
            }
        }

        if (columnName.remaining() > 0) {
            ByteBuffer data = getWithShortLength(columnName);
            if (data != null && data.remaining() > 0) {
                byte[] bytes = BYTE_BUFFER_CODEC.encode(data);
                String valueName = STRING_CODEC.decode(bytes);
                FieldMapper<?> valueMapper = valueMappers.get(valueName);
                valueMapper.setField(entity, columnValue);
                byte end_of_component = columnName.get();
                if (end_of_component != CompareOp.EQUAL.toByte()) {
                    throw new RuntimeException("Invalid composite column.  Expected END_OF_COMPONENT.");
                }
            } else {
                throw new RuntimeException("Invalid value name in composite type");
            }
        }
    }

    public static int getShortLength(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    public static ByteBuffer getWithShortLength(ByteBuffer bb) {
        int length = getShortLength(bb);
        return getBytes(bb, length);
    }

    public static ByteBuffer getBytes(ByteBuffer bb, int length) {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    public boolean isEntityMarker(ByteBuffer columnName) {
        return isEntityMarker(getNumComponents(), columnName);
    }

    public int compare(ByteBuffer o1, ByteBuffer o2) {
        return compare(getNumComponents(), o1, o2);
    }

    // The component bytes are the bytes that match the entity marker
    public static ByteBuffer getComponentBytes(int numComponents, ByteBuffer byteBuffer) {
        ByteBuffer bb = byteBuffer.duplicate();
        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int i = 0;
        while (bb.remaining() > 0 && i < numComponents) {
            int length = getShortLength(bb);
            result.putShort((short) length);
            result.put(getBytes(bb, length));

            // consume equality
            byte b = bb.get();
            result.put(b);
            i++;
        }
        result.flip();
        return result;
    }

    public static String getValueName(int numComponents, ByteBuffer byteBuffer) {
        ByteBuffer bb = byteBuffer.duplicate();
        int i = 0;
        while (bb.remaining() > 0 && i < numComponents) {
            getWithShortLength(bb);

            // consume equality
            bb.get();
            i++;
        }
        if (bb.remaining() > 0) {
            ByteBuffer data = getWithShortLength(bb);
            if (data != null && data.remaining() > 0) {
                byte[] bytes = BYTE_BUFFER_CODEC.encode(data);
                return STRING_CODEC.decode(bytes);
            } else {
                throw new RuntimeException("Invalid value name in composite type");
            }
        } else {
            return null;
        }
    }

    public static boolean isEntityMarker(int numComponents, ByteBuffer columnName) {
        return getValueName(numComponents, columnName) == null;
    }

    public static int compare(int numComponents, ByteBuffer o1, ByteBuffer o2) {
        if (o1 == null)
            return o2 == null ? 0 : -1;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();

        int i = 0;
        while (bb1.remaining() > 0 && bb2.remaining() > 0 && i < numComponents) {
            ByteBuffer value1 = getWithShortLength(bb1);
            ByteBuffer value2 = getWithShortLength(bb2);

            int cmp = BytesUtil.compareTo(value1, value2);

            if (cmp != 0)
                return cmp;

            byte b1 = bb1.get();
            byte b2 = bb2.get();
            if (b1 < 0) {
                if (b2 >= 0)
                    return -1;
            } else if (b1 > 0) {
                if (b2 <= 0)
                    return 1;
            } else {
                // b1 == 0
                if (b2 != 0)
                    return -b2;
            }
            i++;
        }

        if (i >= numComponents) {
            return 0;
        }

        if (bb1.remaining() == 0) {
            return bb2.remaining() == 0 ? 0 : -1;
        }

        // bb1.remaining() > 0 && bb2.remaining() == 0
        return 1;
    }

    ByteBuffer[] getQueryEndpoints(Collection<ColumnPredicate> predicates) {
        // Convert to multimap for easy lookup
        ArrayListMultimap<Object, ColumnPredicate> lookup = ArrayListMultimap.create();
        for (ColumnPredicate predicate : predicates) {
            Preconditions.checkArgument(componentNames.contains(predicate.getName()), "Field '" + predicate.getName() + "' is not a valid element ID");
            lookup.put(predicate.getName(), predicate);
        }

        CompositeBuilder start = new CompositeBuilder(BUFFER_SIZE, CompareOp.GREATER_THAN_EQUAL);
        CompositeBuilder end = new CompositeBuilder(BUFFER_SIZE, CompareOp.LESS_THAN_EQUAL);

        // Iterate through components in order while applying predicate to 'start' and 'end'
        for (FieldMapper<?> mapper : components) {
            for (ColumnPredicate p : lookup.get(mapper.getName())) {
                try {
                    applyPredicate(mapper, start, end, p);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Failed to serialize predicate '%s'", p.toString()), e);
                }
            }
        }

        return new ByteBuffer[]{start.get(), end.get()};
    }

    void applyPredicate(FieldMapper<?> mapper, CompositeBuilder start, CompositeBuilder end,
                        ColumnPredicate predicate) {
        ByteBuffer bb = predicate.getValue();

        switch (predicate.getOp()) {
            case EQUAL:
                start.addWithoutControl(bb);
                end.addWithoutControl(bb);
                break;
            case GREATER_THAN:
            case GREATER_THAN_EQUAL:
                if (mapper.isAscending()) {
                    start.add(bb, predicate.getOp());
                } else {
                    end.add(bb, predicate.getOp().reverse());
                }
                break;
            case LESS_THAN:
            case LESS_THAN_EQUAL:
                if (mapper.isAscending()) {
                    end.add(bb, predicate.getOp());
                } else {
                    start.add(bb, predicate.getOp().reverse());
                }
                break;
        }
    }
}
