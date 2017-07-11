package io.hentitydb.entity;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ColumnFamily {
    /**
     * The name of the column family.
     */
    String name();

    /**
     * The time-to-live in seconds for columns in the column family.
     */
    int ttl() default -1;

    /**
     * The maximum number of entities to store per row in the column family.
     */
    int maxEntitiesPerRow() default -1;

    /**
     * The time-to-live in seconds for an entity if the maximum number of entities are present,
     * if the ttl has not been met, the entity is not removed even if the maximum number exist.
     */
    int maxEntitiesPerRowTtl() default 0;

    /**
     * Another column family in the same row which references this column family.
     *
     * It is assumed that entities in this family refer to entities with the same ID (and element IDs if specified)
     * in the referenced family.
     */
    String referencingFamily() default "";

    /**
     * Another another column family in the same row which indexes this column family.
     *
     * It is assumed that entities in this family have a value (for the specified indexing value name)
     * that refers to entities with the same ID and with a single element ID that equals the value.
     * Currently the single element ID must be of type Long.
     */
    String indexingFamily() default "";

    /**
     * The value name containing the index value.
     *
     * It is assumed that entities in this family have a value (for the given value name) that refers to entities
     * with the same ID and with a single element ID that equals the value.
     * Currently the single element ID must be of type Long.
     */
    String indexingValueName() default "";
}
