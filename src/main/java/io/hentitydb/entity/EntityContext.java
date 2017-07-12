package io.hentitydb.entity;

import io.hentitydb.store.Table;

import javax.persistence.PersistenceException;
import java.util.Collection;
import java.util.List;

public interface EntityContext<T, K> {

    /**
     * Returns the table.
     *
     * @return the table
     */
    Table<K, byte[]> getTable() throws PersistenceException;

    /**
     * Writes the entity.
     *
     * @param entity entity
     */
    void put(T entity) throws PersistenceException;

    /**
     * Writes the entities.  Entities with the same ID are written atomically.
     *
     * @param entities entities
     */
    void put(Collection<T> entities) throws PersistenceException;

    /**
     * Retrieves all entities.
     *
     * @return the entities
     */
    List<T> getAll() throws PersistenceException;

    /**
     * Retrieves the entities with the given row key.
     *
     * @param id row key
     * @return the entities
     */
    List<T> get(K id) throws PersistenceException;

    /**
     * Deletes the row with the given row key.
     *
     * @param id row key
     */
    void delete(K id) throws PersistenceException;

    /**
     * Removes the specified entity from the row.
     *
     * @param entity entity
     */
    void remove(T entity) throws PersistenceException;

    /**
     * Removes the specified entities from the row.  Entities with the same ID are removed atomically.
     *
     * @param entities entities
     */
    void remove(Collection<T> entities) throws PersistenceException;

    /**
     * Create a select query to return zero or more entities.
     *
     * @return the query
     */
    SelectQuery<T, K> select();

    /**
     * Create a get query to return a single entity.
     *
     * @return the query
     */
    SelectOneQuery<T, K> selectOne();

    /**
     * Create an update query.
     *
     * @return the query
     */
    UpdateQuery<T, K> update();

    /**
     * Create a delete query.
     *
     * @return the query
     */
    DeleteQuery<T, K> delete();

    /**
     * Create a mutations query to atomically execute more than one mutation query for a given row.
     *
     * @return the query
     */
    MutationsQuery<T, K> mutate();

    /**
     * Truncate all data.
     */
    void truncate() throws PersistenceException;
}
