package io.hentitydb.entity;

import io.hentitydb.serialization.AbstractCodec;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A query predicate.
 */
public abstract class QueryPredicate extends AbstractCodec<QueryPredicate> {

    public abstract boolean evaluate(Map<String, ByteBuffer> entity);

    @Override
    public void encode(QueryPredicate value, WriteBuffer buffer) {
        encode(buffer);
    }

    public abstract void encode(WriteBuffer buffer);

    public abstract QueryPredicate decode(ReadBuffer buffer);
}
