package io.pravega.connectors.spark;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PravegaRelationTest {
    PravegaReader p = new PravegaReader();

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void schema() {
        p.pravegaSchema()
    }

    @Test
    public void sqlContext() {
    }

    @Test
    public void buildScan() {
    }
}