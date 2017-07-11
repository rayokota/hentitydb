package io.hentitydb.store;

public enum BooleanOp {
    AND((byte) 1), OR((byte) 0), NOT((byte) -1);

    private final byte op;

    BooleanOp(byte op) {
        this.op = op;
    }

    public byte toByte() {
        return op;
    }

    public static BooleanOp fromByte(byte op) {
        if (op > 0) {
            return AND;
        }
        if (op < 0) {
            return NOT;
        }
        return OR;
    }
}
