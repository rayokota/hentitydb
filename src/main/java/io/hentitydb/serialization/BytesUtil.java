package io.hentitydb.serialization;

import java.nio.ByteBuffer;

public class BytesUtil {

    public static int compareTo(byte[] buffer1, byte[] buffer2) {
        return compareTo(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
    }

    public static int compareTo(byte[] buffer1, int offset1, int length1,
                                byte[] buffer2, int offset2, int length2) {
        // Short circuit equal case
        if (buffer1 == buffer2 &&
                offset1 == offset2 &&
                length1 == length2) {
            return 0;
        }
        int end1 = offset1 + length1;
        int end2 = offset2 + length2;
        for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
            int a = (buffer1[i] & 0xff);
            int b = (buffer2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return length1 - length2;
    }

    public static int compareTo(ByteBuffer buffer1, ByteBuffer buffer2) {
        int n = buffer1.position() + Math.min(buffer1.remaining(), buffer2.remaining());
        for (int i = buffer1.position(), j = buffer2.position(); i < n; i++, j++) {
            int a = (buffer1.get(i) & 0xff);
            int b = (buffer2.get(j) & 0xff);
            int cmp = a - b;
            if (cmp != 0) {
                return cmp;
            }
        }
        return buffer1.remaining() - buffer2.remaining();
    }
}
