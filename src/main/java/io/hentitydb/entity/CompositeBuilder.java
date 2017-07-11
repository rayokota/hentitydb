package io.hentitydb.entity;

import com.google.common.base.Preconditions;
import io.hentitydb.store.CompareOp;

import java.nio.ByteBuffer;

public class CompositeBuilder {
    private final static int COMPONENT_OVERHEAD = 3;

    private int bufferSize;
    private ByteBuffer bb;
    private boolean hasControl = true;
    private CompareOp lastCompareOp = CompareOp.EQUAL;
    private final CompareOp finalCompareOp;

    public CompositeBuilder(int bufferSize, CompareOp finalCompareOp) {
        bb = ByteBuffer.allocate(bufferSize);
        this.finalCompareOp = finalCompareOp;
    }

    public void add(ByteBuffer cb, CompareOp control) {
        addWithoutControl(cb);
        addControl(control);
    }

    public void addWithoutControl(ByteBuffer cb) {
        Preconditions.checkState(lastCompareOp == CompareOp.EQUAL, "Cannot extend composite since non equality control already set");

        if (cb == null) {
            cb = ByteBuffer.allocate(0);
        }

        if (cb.limit() + COMPONENT_OVERHEAD > bb.remaining()) {
            int exponent = (int) Math.ceil(Math.log((double) (cb.limit() + COMPONENT_OVERHEAD + bb.limit())) / Math.log(2));
            bufferSize = (int) Math.pow(2, exponent);
            ByteBuffer temp = ByteBuffer.allocate(bufferSize);
            bb.flip();
            temp.put(bb);
            bb = temp;
        }

        if (!hasControl()) {
            addControl(CompareOp.EQUAL);
        } else if (bb.position() > 0) {
            bb.position(bb.position() - 1);
            addControlByte(CompareOp.EQUAL);
        }

        // Write the data: <length><data>
        bb.putShort((short) cb.remaining());
        bb.put(cb.slice());
        hasControl = false;
    }

    public void addControl(CompareOp control) {
        Preconditions.checkState(!hasControl, "Control byte already set");
        Preconditions.checkState(lastCompareOp == CompareOp.EQUAL, "Cannot extend composite since non equality control already set");
        hasControl = true;
        addControlByte(control);
    }

    private void addControlByte(CompareOp control) {
        byte b = control.toByte();
        byte controlByte = b < 0 ? (byte) -1 : (b > 0 ? (byte) 1 : (byte) 0);
        bb.put(controlByte);
    }

    public boolean hasControl() {
        return hasControl;
    }

    public ByteBuffer get() {
        if (!hasControl())
            addControl(this.finalCompareOp);

        ByteBuffer ret = bb.duplicate();
        ret.flip();
        return ret;
    }
}
