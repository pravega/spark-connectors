package io.pravega.connectors.spark;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Test for {@link PravegaRecordToUnsafeRowConverter}
 */
public class PravegaRecordToUnsafeRowConverterTest {

    @Test
    public void toUnsafeRowTest() {
        PravegaRecordToUnsafeRowConverter pravegaRecordToUnsafeRowConverter = new PravegaRecordToUnsafeRowConverter();
        // Case 1
        UnsafeRow x = pravegaRecordToUnsafeRowConverter.toUnsafeRow("event".getBytes(), "scope", "stream", 1L, 2L);
        Assert.assertEquals(x.getLong(3), 1);
        Assert.assertEquals(x.getLong(4), 2);
        Assert.assertEquals(x.getString(1), "scope");
        Assert.assertEquals(x.getString(2), "stream");
        Assert.assertEquals(x.getString(0), "event");

        //Case 2
        UnsafeRow y = pravegaRecordToUnsafeRowConverter.toUnsafeRow(("e04fd020ea3a6910a2d808002b30309d").getBytes(), "PravegaScope", "StreamName", 12345678910L, 457865441129309L);
        Assert.assertEquals(y.getString(0), "e04fd020ea3a6910a2d808002b30309d");
        Assert.assertEquals(y.getString(1), "PravegaScope");
        Assert.assertEquals(y.getString(2), "StreamName");
        Assert.assertEquals(y.getLong(3), 12345678910L);
        Assert.assertEquals(y.getLong(4), 457865441129309L);

        //Case 3
        UnsafeRow z = pravegaRecordToUnsafeRowConverter.toUnsafeRow("".getBytes(), "", "", 0, -1);
        Assert.assertEquals(z.getString(0), "");
        Assert.assertEquals(z.getString(1), "");
        Assert.assertEquals(z.getString(2), "");
        Assert.assertEquals(z.getLong(3), 0);
        Assert.assertEquals(z.getLong(4), -1);
    }
}

