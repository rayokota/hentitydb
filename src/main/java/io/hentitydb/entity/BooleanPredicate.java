package io.hentitydb.entity;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.hentitydb.serialization.ClassCodec;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;
import io.hentitydb.store.BooleanOp;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class BooleanPredicate extends QueryPredicate {

    private final static ClassCodec CLASS_CODEC = new ClassCodec();

    private BooleanOp op;
    private List<QueryPredicate> predicates = Lists.newArrayList();

    public BooleanOp getOp() {
        return op;
    }

    public List<QueryPredicate> getPredicates() {
        return predicates;
    }

    public BooleanPredicate setOp(BooleanOp op) {
        this.op = op;
        return this;
    }

    public BooleanPredicate addPredicate(QueryPredicate predicate) {
        predicates.add(predicate);
        return this;
    }

    public boolean evaluate(Map<String, ByteBuffer> entity) {
        switch (getOp()) {
            case AND:
                for (QueryPredicate p : getPredicates()) {
                    if (!p.evaluate(entity)) return false;
                }
                return true;
            case OR:
                for (QueryPredicate p : getPredicates()) {
                    if (p.evaluate(entity)) return true;
                }
                return false;
            case NOT:
                if (getPredicates().size() != 1) throw new IllegalArgumentException();
                return !getPredicates().get(0).evaluate(entity);
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void encode(WriteBuffer buffer) {
        buffer.writeByte(getOp().toByte());
        buffer.writeVarInt(predicates.size());
        for (QueryPredicate predicate : predicates) {
            CLASS_CODEC.encode(predicate.getClass(), buffer);
            predicate.encode(buffer);
        }
    }

    @Override
    public BooleanPredicate decode(ReadBuffer buffer) {
        try {
            BooleanPredicate result = new BooleanPredicate();
            final BooleanOp op = BooleanOp.fromByte(buffer.readByte());
            result.setOp(op);
            final int size = buffer.readVarInt();
            for (int i = 0; i < size; i++) {
                final Class predicateClass = CLASS_CODEC.decode(buffer);
                QueryPredicate predicate = (QueryPredicate) predicateClass.newInstance();
                predicate = predicate.decode(buffer);
                result.addPredicate(predicate);
            }
            return result;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
