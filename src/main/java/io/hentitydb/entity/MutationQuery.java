package io.hentitydb.entity;

public interface MutationQuery<T, K> {

    K getId();

    interface MutationIdQuery<T, K> {
        MutationQuery<T, K> eq(K key);
    }

    MutationIdQuery<T, K> whereId();

    MutationQuery<T, K> ifColumnFamily(String name);

    interface MutateIfElementIdQuery<T, K> {
        MutationQuery<T, K> eq(Object value);
    }

    MutateIfElementIdQuery<T, K> ifElementId(String name);

    interface MutateIfColumnQuery<T, K> {

        MutationQuery<T, K> eq(Object value);

        MutationQuery<T, K> gt(Object value);

        MutationQuery<T, K> lt(Object value);

        MutationQuery<T, K> gte(Object value);

        MutationQuery<T, K> lte(Object value);

        MutationQuery<T, K> isNull();

        MutationQuery<T, K> isAbsent();
    }

    MutateIfColumnQuery<T, K> ifColumn(String family);

    boolean execute();
}
