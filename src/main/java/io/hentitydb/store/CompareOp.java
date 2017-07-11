package io.hentitydb.store;

public enum CompareOp {
    LESS_THAN((byte) -2), GREATER_THAN_EQUAL((byte) -1), EQUAL((byte) 0), GREATER_THAN((byte) 1), LESS_THAN_EQUAL(
            (byte) 2);

    private final byte equality;

    CompareOp(byte equality) {
        this.equality = equality;
    }

    public byte toByte() {
        return equality;
    }

    public CompareOp reverse() {
        switch (this) {
            case LESS_THAN:
                return GREATER_THAN;
            case GREATER_THAN_EQUAL:
                return LESS_THAN_EQUAL;
            case EQUAL:
                return EQUAL;
            case GREATER_THAN:
                return LESS_THAN;
            case LESS_THAN_EQUAL:
                return GREATER_THAN_EQUAL;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static CompareOp fromByte(byte equality) {
        switch (equality) {
            case -2:
                return LESS_THAN;
            case -1:
                return GREATER_THAN_EQUAL;
            case 0:
                return EQUAL;
            case 1:
                return GREATER_THAN;
            case 2:
                return LESS_THAN_EQUAL;
            default:
                throw new IllegalArgumentException();
        }
    }
}
