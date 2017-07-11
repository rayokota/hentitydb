package io.hentitydb.entity;

import com.google.common.collect.Maps;
import io.hentitydb.store.CompareOp;
import io.hentitydb.store.Put;
import io.hentitydb.store.Table;

import java.nio.ByteBuffer;
import java.util.Map;

public abstract class UpdateQuery<T, K> implements MutationQuery<T, K> {
    protected String family;
    protected K id;
    protected final Map<String, Object> elementIds = Maps.newHashMap();
    protected final Map<String, Object> setColumns = Maps.newHashMap();
    protected String ifFamily;
    protected final Map<String, Object> ifEqualsElementIds = Maps.newHashMap();
    protected ColumnPredicate ifColumnPredicate;

    public UpdateQuery<T, K> fromColumnFamily(String family) {
        this.family = family;
        return this;
    }

    public K getId() {
        return id;
    }

    public class UpdateIdQuery implements MutationQuery.MutationIdQuery<T, K>{
        public UpdateQuery<T, K> eq(K key) {
            id = key;
            return UpdateQuery.this;
        }
    }

    public class UpdateElementIdQuery implements MutationQuery.MutateIfElementIdQuery<T, K> {
        private final String name;

        public UpdateElementIdQuery(String name) {
            this.name = name;
        }

        public UpdateQuery<T, K> eq(Object value) {
            elementIds.put(name, value);
            return UpdateQuery.this;
        }
    }

    public UpdateQuery<T, K> setColumn(String name, Object value) {
        setColumns.put(name, value);
        return this;
    }

    public UpdateIdQuery whereId() {
        return new UpdateIdQuery();
    }

    public UpdateElementIdQuery whereElementId(String name) {
        return new UpdateElementIdQuery(name);
    }

    public UpdateQuery<T, K> ifColumnFamily(String family) {
        this.ifFamily = family;
        return this;
    }

    public class UpdateIfElementIdQuery implements MutationQuery.MutateIfElementIdQuery<T, K> {
        private final String name;

        public UpdateIfElementIdQuery(String name) {
            this.name = name;
        }

        public UpdateQuery<T, K> eq(Object value) {
            ifEqualsElementIds.put(name, value);
            return UpdateQuery.this;
        }
    }

    public UpdateIfElementIdQuery ifElementId(String name) {
        return new UpdateIfElementIdQuery(name);
    }

    public class UpdateIfColumnQuery implements MutationQuery.MutateIfColumnQuery<T, K>{
        private final ColumnPredicate predicate = new ColumnPredicate();

        public UpdateIfColumnQuery(String name) {
            predicate.setName(name);
        }

        public UpdateQuery<T, K> eq(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public UpdateQuery<T, K> gt(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.GREATER_THAN).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public UpdateQuery<T, K> lt(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.LESS_THAN).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public UpdateQuery<T, K> gte(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.GREATER_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public UpdateQuery<T, K> lte(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.LESS_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public UpdateQuery<T, K> isNull() {
            return setIfColumnPredicate(predicate.setOp(CompareOp.EQUAL).setValue(new byte[0]));
        }

        public UpdateQuery<T, K> isAbsent() {
            return setIfColumnPredicate(predicate);
        }
    }

    private UpdateQuery<T, K> setIfColumnPredicate(ColumnPredicate predicate) {
        if (ifColumnPredicate != null) {
            // only one non-id column predicate allowed
            throw new IllegalArgumentException("Non-ID column predicate already set");
        }
        this.ifColumnPredicate = predicate;
        return this;
    }

    public UpdateIfColumnQuery ifColumn(String name) {
        return new UpdateIfColumnQuery(name);
    }

    /**
     * Creates a raw put operation.
     *
     * @param table table
     */
    protected abstract Put<K, byte[]> rawPut(Table<K, byte[]> table);

    public abstract boolean execute();

    protected abstract ByteBuffer getRawValue(String name, Object value, boolean isComponent);
}
