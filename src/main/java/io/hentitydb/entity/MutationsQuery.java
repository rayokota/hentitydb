package io.hentitydb.entity;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.hentitydb.store.CompareOp;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public abstract class MutationsQuery<T, K> implements MutationQuery<T, K> {
    protected K id;
    protected final List<MutationQuery<T, K>> mutations = Lists.newArrayList();
    protected String ifFamily;
    protected final Map<String, Object> ifEqualsElementIds = Maps.newHashMap();
    protected ColumnPredicate ifColumnPredicate;

    public K getId() {
        return id;
    }

    public class MutationsIdQuery implements MutationQuery.MutationIdQuery<T, K>{
        public MutationsQuery<T, K> eq(K key) {
            id = key;
            return MutationsQuery.this;
        }
    }

    public MutationsIdQuery whereId() {
        return new MutationsIdQuery();
    }

    public MutationsQuery<T, K> add(MutationQuery<T, K>... queries) {
        if (id == null) {
            throw new IllegalStateException("ID is not set");
        }
        for (MutationQuery<T, K> mutation : queries) {
            if (!id.equals(mutation.getId())) {
                throw new IllegalArgumentException("Mutation ID does not match this ID");
            }
            mutations.add(mutation);
        }
        return this;
    }

    public MutationsQuery<T, K> ifColumnFamily(String family) {
        this.ifFamily = family;
        return this;
    }

    public class MutationsIfElementIdQuery implements MutationQuery.MutateIfElementIdQuery<T, K> {
        private final String name;

        public MutationsIfElementIdQuery(String name) {
            this.name = name;
        }

        public MutationsQuery<T, K> eq(Object value) {
            ifEqualsElementIds.put(name, value);
            return MutationsQuery.this;
        }
    }

    public MutationsIfElementIdQuery ifElementId(String name) {
        return new MutationsIfElementIdQuery(name);
    }

    public class MutationsIfColumnQuery implements MutationQuery.MutateIfColumnQuery<T, K>{
        private final ColumnPredicate predicate = new ColumnPredicate();

        public MutationsIfColumnQuery(String name) {
            predicate.setName(name);
        }

        public MutationsQuery<T, K> eq(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public MutationsQuery<T, K> gt(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.GREATER_THAN).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public MutationsQuery<T, K> lt(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.LESS_THAN).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public MutationsQuery<T, K> gte(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.GREATER_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public MutationsQuery<T, K> lte(Object value) {
            return setIfColumnPredicate(predicate.setOp(CompareOp.LESS_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public MutationsQuery<T, K> isNull() {
            return setIfColumnPredicate(predicate.setOp(CompareOp.EQUAL).setValue(new byte[0]));
        }

        public MutationsQuery<T, K> isAbsent() {
            return setIfColumnPredicate(predicate);
        }
    }

    private MutationsQuery<T, K> setIfColumnPredicate(ColumnPredicate predicate) {
        if (ifColumnPredicate != null) {
            // only one non-id column predicate allowed
            throw new IllegalArgumentException("Non-ID column predicate already set");
        }
        this.ifColumnPredicate = predicate;
        return this;
    }

    public MutationsIfColumnQuery ifColumn(String name) {
        return new MutationsIfColumnQuery(name);
    }

    public abstract boolean execute();

    protected abstract ByteBuffer getRawValue(String name, Object value, boolean isComponent);
}

