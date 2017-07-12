package io.hentitydb.entity;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.hentitydb.store.CompareOp;
import io.hentitydb.store.Delete;
import io.hentitydb.store.Table;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public abstract class DeleteQuery<T, K> implements MutationQuery<T, K> {
    protected String family;
    protected K id;
    protected final Map<String, Object> elementIds = Maps.newHashMap();
    protected final List<String> columns = Lists.newArrayList();
    protected String ifFamily;
    protected final Map<String, Object> ifEqualsElementIds = Maps.newHashMap();
    protected ColumnPredicate ifColumnPredicate;

    public DeleteQuery<T, K> fromColumnFamily(String family) {
        this.family = family;
        return this;
    }

    public K getId() {
        return id;
    }

    public class DeleteIdQuery implements MutationQuery.MutationIdQuery<T, K>{
        public DeleteQuery<T, K> eq(K key) {
            id = key;
            return DeleteQuery.this;
        }
    }

    public class DeleteElementIdQuery {
        private final String name;

        public DeleteElementIdQuery(String name) {
            this.name = name;
        }

        public DeleteQuery<T, K> eq(Object value) {
            elementIds.put(name, value);
            return DeleteQuery.this;
        }
    }

    public DeleteQuery<T, K> deleteColumn(String name) {
        columns.add(name);
        return this;
    }

    public DeleteIdQuery whereId() {
        return new DeleteIdQuery();
    }

    public DeleteElementIdQuery whereElementId(String name) {
        return new DeleteElementIdQuery(name);
    }

    public DeleteQuery<T, K> ifColumnFamily(String family) {
        this.ifFamily = family;
        return this;
    }

    public class DeleteIfElementIdQuery implements MutationQuery.MutateIfElementIdQuery<T, K> {
        private final String name;

        public DeleteIfElementIdQuery(String name) {
            this.name = name;
        }

        public DeleteQuery<T, K> eq(Object value) {
            ifEqualsElementIds.put(name, value);
            return DeleteQuery.this;
        }
    }

    public DeleteIfElementIdQuery ifElementId(String name) {
        return new DeleteIfElementIdQuery(name);
    }

    public class DeleteIfColumnQuery implements MutationQuery.MutateIfColumnQuery<T, K> {
        private final ColumnPredicate predicate = new ColumnPredicate();

        public DeleteIfColumnQuery(String name) {
            predicate.setName(name);
        }

        public DeleteQuery<T, K> eq(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public DeleteQuery<T, K> gt(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.GREATER_THAN).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public DeleteQuery<T, K> lt(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.LESS_THAN).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public DeleteQuery<T, K> gte(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.GREATER_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public DeleteQuery<T, K> lte(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.LESS_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public DeleteQuery<T, K> isNull() {
            return setIfColumnPredicate(predicate.setOp(CompareOp.EQUAL).setValue(new byte[0]));
        }

        public DeleteQuery<T, K> isAbsent() {
            return setIfColumnPredicate(predicate);
        }
    }

    private DeleteQuery<T, K> setIfColumnPredicate(ColumnPredicate predicate) {
        if (ifColumnPredicate != null) {
            // only one non-id column predicate allowed
            throw new IllegalArgumentException("Non-ID column predicate already set");
        }
        this.ifColumnPredicate = predicate;
        return this;
    }

    public DeleteIfColumnQuery ifColumn(String name) {
        return new DeleteIfColumnQuery(name);
    }

    public abstract boolean execute();

    /**
     * Creates a raw delete operation.
     *
     * @param table table
     * @return the raw delete
     */
    protected abstract Delete<K, byte[]> rawRemove(Table<K, byte[]> table);

    protected abstract ByteBuffer getRawValue(String name, Object value, boolean isComponent);
}
