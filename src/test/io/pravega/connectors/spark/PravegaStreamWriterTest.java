package io.pravega.connectors.spark;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.*;
import io.pravega.client.ClientFactory;
import io.pravega.client.ClientConfig;

import org.apache.spark.sql.types.StructType;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import org.mockito.Mock;
import org.mockito.Mockito;



public class PravegaStreamWriterTest {


    /**
     * @param transactionTimeout  The number of milliseconds for transactions to timeout.
     * @param schema                   The schema of the input data.
     */

    private static final ClientConfig CLIENT_CONFIG = ClientConfig.builder().build();
    private static final String SCOPE_NAME = "scope";
    private static final String STREAM_NAME = "stream";
    private static final String ROUTING_KEY = "key";
    private static final StructType Schema  = PravegaReader.pravegaSchema();
    private UUID txnId ;
    final long transactionTimeout = 5000;

    @Mock
    EventStreamWriter<String> pravegaWriter = mockEventStreamWriter();
    @Mock
    ClientFactory clientFactory = mockClientFactory();


    @Mock
    Transaction<String> txn = mockTransaction();
    PravegaWriterCommitMessage msg;


    /**
     *
     * @throws TxnFailedException
     * @throws InterruptedException
     */
    @Test
    public void commit() throws TxnFailedException, InterruptedException {



        msg = new PravegaWriterCommitMessage(txnId);

        when(clientFactory.<String>createEventWriter(anyObject(),anyObject(),anyObject())).thenReturn(pravegaWriter);
        when(pravegaWriter.beginTxn()).thenReturn(txn);

        txn.writeEvent(ROUTING_KEY,"HELLO PRAVEGA");
        txn.commit();


        Thread.sleep(2000);
        verify(txn,times(1)).commit();




        pravegaWriter.close();
        pravegaWriter.flush();
        txn.flush();
        verify(pravegaWriter).flush();
        verify(pravegaWriter).close();
        verify(txn,times(1)).flush();

        assertNotNull(msg);

    }


    Transaction<String> txn2= mockTransaction();

    @Test
    public void abort() throws Exception {

        when(pravegaWriter.beginTxn()).thenReturn(txn2);


        for (int i = 0; i < 5; i++) {
            String event = "HELLO PRAVEGA";
            txn2.writeEvent("", event);
            txn2.flush();
            Thread.sleep(500);
        }

        CompletableFuture<Object> e2 =CompletableFuture.supplyAsync(() -> {
            try {
                txn2.abort();
            } catch (Exception e) {

            }
            return null;
        });
        e2.join();
        verify(txn2,never()).commit();

        pravegaWriter.close();
    }
    @SuppressWarnings("unchecked")
    private EventStreamWriter mockEventStreamWriter() {
        EventStreamWriter writer = mock(EventStreamWriter.class);
        Mockito.doNothing().when(writer).flush();
        Mockito.doNothing().when(writer).close();
        return writer;
    }

    @SuppressWarnings("unchecked")
    private <T> Transaction<T> mockTransaction() {
        return mock(Transaction.class);
    }

    @SuppressWarnings("unchecked")
    private ClientFactory mockClientFactory(){
        return mock(ClientFactory.class);
    }
}