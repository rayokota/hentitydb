package io.hentitydb.entity;

import com.google.common.collect.Maps;

import javax.persistence.PersistenceException;
import java.util.Map;

public abstract class SelectOneQuery<T, K> {
    protected String family;
    protected K id;
    protected final Map<String, Object> elementIds = Maps.newHashMap();

    public SelectOneQuery<T, K> fromColumnFamily(String family) {
        this.family = family;
        return this;
    }

    public class SelectIdQuery {
        public SelectOneQuery<T, K> eq(K key) {
            id = key;
            return SelectOneQuery.this;
        }
    }

    @SuppressWarnings("unchecked")
    public class SelectElementIdQuery {
        private final String name;

        public SelectElementIdQuery(String name) {
            this.name = name;
        }

        public SelectOneQuery<T, K> eq(Object value) {
            elementIds.put(name, value);
            return SelectOneQuery.this;
        }
    }

    public SelectIdQuery whereId() {
        return new SelectIdQuery();
    }

    public SelectElementIdQuery whereElementId(String name) {
        return new SelectElementIdQuery(name);
    }

    /**
     * Return a single entity.
     *
     * @return entity
     */
    public abstract T fetchOne() throws PersistenceException;
}
