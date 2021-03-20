---
title: Getting Started with Spark
sidebar_label: Getting Started
---

<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

import TOCInline from '@theme/TOCInline';
import { IfHaveFeature, IfMissingFeature } from 'nautilus-docs';

<TOCInline toc={toc} />

You can run Apache Spark applications written in Java, Scala, or Python. Using Python is the easiest to get started.

## Prepare Development Environment

<IfHaveFeature feature="nautilus">

:::tip Streaming Data Platform
SDP users can optionally skip this entire *Prepare Development Environment* section. However, it is often useful to develop applications locally in a sandbox environment before deploying to a production system such as SDP. For this reason, we recommend that this section is not skipped.
:::

</IfHaveFeature>

### Install Operating System

Install Ubuntu 20.04 LTS. Other operating systems can also be used but the commands below have only been tested
on this version.

### Install Java 11

```shell
sudo apt-get install openjdk-11-jdk
```

You may have multiple versions of Java installed. Ensure that Java 11 is the default with the command below.

```shell
sudo update-alternatives --config java
```

### Run Pravega

This will run a development instance of Pravega locally. The transaction parameters allow transactions to remain open for up to 30 days without lease renewals.

```shell
cd
git clone https://github.com/pravega/pravega
cd pravega
git checkout r0.9
./gradlew startStandalone \
    -Dcontroller.transaction.lease.count.max=2592000000 \
    -Dcontroller.transaction.execution.timeBound.days=30
```

### Install Apache Spark

This will install a development instance of Spark locally.

Download https://www.apache.org/dyn/closer.lua/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz.

```shell
mkdir -p ~/spark
cd ~/spark
tar -xzvf ~/Downloads/spark-3.0.2-bin-hadoop2.7.tgz
ln -s spark-3.0.2-bin-hadoop2.7 current
export PATH="$HOME/spark/current/bin:$PATH"
```

By default, the script `run_spark_ap.sh` will use an in-process Spark mini-cluster
that is started with the Spark job (`--master local[2]`).

### Clone Pravega Samples Repository

This will download the Pravega samples, which includes the Spark samples.

```shell
cd
git clone https://github.com/pravega/pravega-samples
cd pravega-samples
git checkout spark-connector-examples
cd spark-connector-examples
```

## Tutorial 1 - Writing to Pravega

A simple Python Spark (PySpark) applications will consist of a single `.py` file. Our first application will be [stream_generated_data_to_pravega.py](https://github.com/pravega/pravega-samples/blob/spark-connector-examples/spark-connector-examples/src/main/python/stream_generated_data_to_pravega.py) and it will continuously write a timestamp to a Pravega stream.

1. The first part of the application imports our dependencies, extracts a few environment variables, and obtains a `SparkSession`.

    ```python title="stream_generated_data_to_pravega.py"
    from pyspark.sql import SparkSession
    import os

    controller = os.getenv("PRAVEGA_CONTROLLER_URI", "tcp://127.0.0.1:9090")
    allowCreateScope = os.getenv("PROJECT_NAME") is None
    scope = os.getenv("PRAVEGA_SCOPE", "spark")
    checkPointLocation = os.getenv("CHECKPOINT_DIR",
                                   "/tmp/spark-checkpoints-stream_generated_data_to_pravega")

    spark = (SparkSession
            .builder
            .getOrCreate()
            )
    ```

2. Next, we read from a [rate source](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets), which continuously generates rows containing a timestamp.

    ```python
    (spark
        .readStream
        .format("rate")
        .load()
    ```

3. Next, we will use a SQL expression to define a simple transformation on the rate source output. When writing to Pravega, we will need to provide a string or binary (byte sequence) column named `event`, and optionally a string column named `routing_key`.

    ```python
        .selectExpr("cast(timestamp as string) as event", "cast(value as string) as routing_key")
    ```

4. Next, we will specify that we want to write the output every 3 seconds. When writing to Pravega, you will typically want to use the append output mode.

    ```python
        .writeStream
        .trigger(processingTime="3 seconds")
        .outputMode("append")
    ```

5. Now we specify the target for the output. This will write to the Pravega stream named `streamprocessing1`.

    ```python
        .format("pravega")
        .option("allow_create_scope", allowCreateScope)
        .option("controller", controller)
        .option("scope", scope)
        .option("stream", "streamprocessing1")
        .option("checkpointLocation", checkPointLocation)
    ```

6. Finally, we start the Spark job that we defined and wait for it to complete. Since this is an unbounded job, it will continue to run until stopped by the user.

    ```python title="stream_generated_data_to_pravega.py"
        .start()
        .awaitTermination()
    )
    ```

7. To run this application locally and write to your local development installation of Pravega:

    ```shell
    ./run_pyspark_app.sh src/main/python/stream_generated_data_to_pravega.py
    ```

   This job will continue to run and write events until stopped.

<IfHaveFeature feature="nautilus">

8. To deploy this Python Spark application on SDP:

   1. [Upload Common Artifacts to your Analytics Project](../sdp/analytics/spark/deploying#upload-common-artifacts-to-your-analytics-project).

   2. [Deploy Python Applications using the SDP UI](../sdp/analytics/spark/deploying#deploying-python-applications-using-the-sdp-ui).

</IfHaveFeature>

## Tutorial 2 - Reading from Pravega

In this tutorial, we'll use [stream_pravega_to_console.py](https://github.com/pravega/pravega-samples/blob/spark-connector-examples/spark-connector-examples/src/main/python/stream_pravega_to_console.py).

1. The beginning of this script is the same as `stream_generated_data_to_pravega.py`. Now we are reading from a Pravega stream source.

    ```python title="stream_pravega_to_console.py"
    (spark
        .readStream
        .format("pravega")
        .option("controller", controller)
        .option("allow_create_scope", allowCreateScope)
        .option("scope", scope)
        .option("stream", "streamprocessing1")
    ```

2. The first time that this Spark application runs, we can choose where in the stream to begin reading from. We can choose `earliest` or `latest`. If the previous execution of this Spark application saved a checkpoint in the checkpoint directory, then this option is ignored and the application will resume from exactly where it left off.

    ```python
        .option("start_stream_cut", "earliest")
        .load()
    ```

3. When reading from a Pravega stream, the following columns will be available:

    Column name | Data Type | Description
    ------------|-----------|----------------------------------------------------------------------------
    event       | binary    | The serialized event. If a string was written, this will be a UTF-8 string.
    scope       | string    | The name of the Pravega scope.
    stream      | string    | The name of the Pravega scope.
    segment_id  | long      | The ID of the Pravega segment containing this event.
    offset      | long      | The byte offset in the Pravega segment that contains this event.

4. Since we wrote a string event, we need to cast it from a UTF-8 string to a Spark string.

    ```python
        .selectExpr("cast(event as string)", "scope", "stream", "segment_id", "offset")
    ```

5. Next, we write the output to the console. We will see the result in the Spark driver log.

    ```python
        .writeStream
        .trigger(processingTime="3 seconds")
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
    ```

6. Stateful operations in Spark must periodically write checkpoints which can be used to recover from failures. The checkpoint directory identified by the environment variable `CHECKPOINT_DIR` should be used for this purpose. It should be highly available until the Spark application is deleted. This should be used even for Spark applications which do not use Pravega.

    ```python
        .option("checkpointLocation", checkPointLocation)
    ```

<IfHaveFeature feature="nautilus">

:::tip Streaming Data Platform
When deploying on SDP, the `CHECKPOINT_DIR` environment variable is automatically set to an appropriate location.
:::

</IfHaveFeature>

## Where To Go Next

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/index.html)
