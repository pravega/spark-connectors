package io.pravega.connectors.spark;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;

import org.junit.Assert;
import org.junit.Test;

/**
 *Unit Test for {@link PravegaRecordToUnsafeRowConverter}
 */
public class PravegaRecordToUnsafeRowConverterTest {

    private PravegaRecordToUnsafeRowConverter pravegaRecordToUnsafeRowConverter=new PravegaRecordToUnsafeRowConverter();

    @Test
    public void toUnsafeRowTest(){
        UnsafeRow x =expected();
        UnsafeRow y =pravegaRecordToUnsafeRowConverter.toUnsafeRow("event".getBytes(),"scope","stream",1L,2L);
        Assert.assertEquals("Both return the same value",x,y);
    }
    /**
     * Expected UnsafeRow
     */
    public UnsafeRow expected(){
        UnsafeRowWriter writer=new UnsafeRowWriter(5);
        writer.reset();
        writer.write(0,"event".getBytes());
        writer.write(1, UTF8String.fromString("scope"));
        writer.write(2, UTF8String.fromString("stream"));
        writer.write(3,1L);
        writer.write(4,2L);
        return writer.getRow();
    }
}