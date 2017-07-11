package io.hentitydb.serialization.tests;

import io.hentitydb.serialization.BigDecimalCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BigDecimalCodecTest {
    private final BigDecimalCodec codec = new BigDecimalCodec(true);
    private final BigDecimalCodec codec2 = new BigDecimalCodec(false);
    private final byte[] bytes = new byte[]{ 0, 0, 0, 2, 4, 4, -46 };
    private final byte[] bytes2 = new byte[]{ 0, 0, 0, 2, 4, -46 };

    @Test
    public void writesBigDecimals() throws Exception {
        assertThat(codec.encode(new BigDecimal("12.34")),
                   is(bytes));
        assertThat(codec2.encode(new BigDecimal("12.34")),
                   is(bytes2));

        assertThat(Bytes.toBytes(new BigDecimal("12.34")),
                   is(bytes2));
    }

    @Test
    public void readsBigDecimals() throws Exception {
        assertThat(codec.decode(bytes),
                   is(new BigDecimal("12.34")));
        assertThat(codec2.decode(bytes2),
                   is(new BigDecimal("12.34")));

        assertThat(Bytes.toBigDecimal(bytes2),
                   is(new BigDecimal("12.34")));
    }
}
