package io.pravega.connectors.spark;

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Assert;
import org.junit.Test;


/**
 * To check if the Pravega Events to UnsafeRow works correctly
 */
public class PravegaRecordToUnsafeRowConverterTest {

    UnsafeRowWriter writer = new UnsafeRowWriter(2);

    private int DEFAULT_CAPACITY = 1 << 16;

    @Test
    public void toUnsafeRow() {


        String  x="Pravega";
        String  y="Spark";

        writer.reset();
        writer.write(0,UTF8String.fromString(x));
        writer.write(1,UTF8String.fromString(y));

        writer.getRow();
        Assert.assertNotEquals(2,writer.totalSize());
        Assert.assertTrue(writer.totalSize()< DEFAULT_CAPACITY);
        Assert.assertFalse(writer.totalSize()  < 1<<4);

    }
}