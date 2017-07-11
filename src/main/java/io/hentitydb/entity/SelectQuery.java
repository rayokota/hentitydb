package io.hentitydb.entity;

import com.google.common.collect.Lists;
import io.hentitydb.store.CompareOp;
import io.hentitydb.store.BooleanOp;

import javax.persistence.PersistenceException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class SelectQuery<T, K> {
    protected K id;
    protected String family;
    protected List<ColumnPredicate> elementIdPredicates;
    protected BooleanPredicate columnPredicate;
    protected int entityLimit = -1;

    public SelectQuery<T, K> fromColumnFamily(String family) {
        this.family = family;
        return this;
    }

    public class SelectIdQuery {
        public SelectQuery<T, K> eq(K key) {
            id = key;
            return SelectQuery.this;
        }
    }

    @SuppressWarnings("unchecked")
    public class SelectElementIdQuery {
        private final ColumnPredicate predicate = new ColumnPredicate();

        public SelectElementIdQuery(String name) {
            predicate.setName(name);
        }

        public SelectQuery<T, K> eq(Object value) {
            return addElementIdPredicate(predicate.setOp(CompareOp.EQUAL).setValue(getRawValue(predicate.getName(), value, true)));
        }

        public SelectQuery<T, K> gt(Object value) {
            return addElementIdPredicate(predicate.setOp(CompareOp.GREATER_THAN).setValue(getRawValue(predicate.getName(), value, true)));
        }

        public SelectQuery<T, K> lt(Object value) {
            return addElementIdPredicate(predicate.setOp(CompareOp.LESS_THAN).setValue(getRawValue(predicate.getName(), value, true)));
        }

        public SelectQuery<T, K> gte(Object value) {
            return addElementIdPredicate(predicate.setOp(CompareOp.GREATER_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, true)));
        }

        public SelectQuery<T, K> lte(Object value) {
            return addElementIdPredicate(predicate.setOp(CompareOp.LESS_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, true)));
        }
    }

    public class SelectColumnQuery {
        private final ColumnPredicate predicate = new ColumnPredicate();

        public SelectColumnQuery(String name) {
            predicate.setName(name);
        }

        public SelectQuery<T, K> eq(Object value) {
            return addColumnPredicate(predicate.setOp(CompareOp.EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public SelectQuery<T, K> neq(Object value) {
            return addColumnPredicate(new BooleanPredicate().setOp(BooleanOp.NOT).addPredicate(
                    predicate.setOp(CompareOp.EQUAL).setValue(getRawValue(predicate.getName(), value, false))));
        }

        public SelectQuery<T, K> gt(Object value) {
            return addColumnPredicate(predicate.setOp(CompareOp.GREATER_THAN).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public SelectQuery<T, K> lt(Object value) {
            return addColumnPredicate(predicate.setOp(CompareOp.LESS_THAN).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public SelectQuery<T, K> gte(Object value) {
            return addColumnPredicate(predicate.setOp(CompareOp.GREATER_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public SelectQuery<T, K> lte(Object value) {
            return addColumnPredicate(predicate.setOp(CompareOp.LESS_THAN_EQUAL).setValue(getRawValue(predicate.getName(), value, false)));
        }

        public SelectQuery<T, K> isNull() {
            return addColumnPredicate(predicate.setOp(CompareOp.EQUAL).setValue(new byte[0]));
        }

        public SelectQuery<T, K> isAbsent() {
            return addColumnPredicate(predicate);
        }
    }

    public SelectIdQuery whereId() {
        return new SelectIdQuery();
    }

    public SelectElementIdQuery whereElementId(String name) {
        return new SelectElementIdQuery(name);
    }

    public SelectColumnQuery whereColumn(String name) {
        return new SelectColumnQuery(name);
    }

    public SelectQuery<T, K> limit(int entityLimit) {
        this.entityLimit = entityLimit;
        return this;
    }

    private SelectQuery<T, K> addElementIdPredicate(ColumnPredicate predicate) {
        if (elementIdPredicates == null) {
            elementIdPredicates = Lists.newArrayList();
        }

        elementIdPredicates.add(predicate);
        return this;
    }

    public SelectQuery<T, K> addColumnPredicate(QueryPredicate predicate) {
        if (columnPredicate == null) {
            columnPredicate = new BooleanPredicate().setOp(BooleanOp.AND);
        }

        columnPredicate.addPredicate(predicate);
        return this;
    }

    /**
     * Return a single entity.
     *
     * @return entity
     */
    public abstract T fetchOne() throws PersistenceException;

    /**
     * Return a collection of entities.
     *
     * @return entities
     */
    public abstract List<T> fetch() throws PersistenceException;

    /**
     * Return a count of entities.
     *
     * @return count
     */
    public abstract int count() throws PersistenceException;

    protected abstract ByteBuffer getRawValue(String name, Object value, boolean isComponent);
}
