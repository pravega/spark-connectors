---
title: Tutorial 1 - Writing to Pravega
---

<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
-->

A simple Python Spark (PySpark) application will consist of a single `.py` file. Our first application will be [stream_generated_data_to_pravega.py](https://github.com/pravega/pravega-samples/blob/spark-connector-examples/spark-connector-examples/src/main/python/stream_generated_data_to_pravega.py) and it will continuously write a timestamp to a Pravega stream.

## Code Walkthrough

1. The first part of the application imports our dependencies.

    ```python title="stream_generated_data_to_pravega.py"
    from pyspark.sql import SparkSession
    import os
    ```

2. In a production deployment, environment variables will be used to pass environment-specific parameters to this application. This application will attempt to get these environment variables, but if they do not exist, it will use defaults that will typically work in a local development environment.

    ```python
    controller = os.getenv("PRAVEGA_CONTROLLER_URI", "tcp://127.0.0.1:9090")
    allowCreateScope = os.getenv("PROJECT_NAME") is None
    scope = os.getenv("PRAVEGA_SCOPE", "spark")
    checkPointLocation = os.getenv("CHECKPOINT_DIR",
                                   "/tmp/spark-checkpoints-stream_generated_data_to_pravega")
    ```

3. We now obtain a `SparkSession`.

    ```python
    spark = (SparkSession
            .builder
            .getOrCreate()
            )
    ```

4. Next, we read from a [rate source](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets), which continuously generates rows containing a timestamp.

    ```python
    (spark
        .readStream
        .format("rate")
        .load()
    ```

5. Next, we will use a SQL expression to define a simple transformation on the rate source output. When writing to Pravega, we will need to provide a string or binary (byte sequence) column named `event`, and optionally a string column named `routing_key`.

    ```python
        .selectExpr("cast(timestamp as string) as event", "cast(value as string) as routing_key")
    ```

6. Next, we will specify that we want to write the output every 3 seconds. When writing to Pravega, you will typically want to use the append output mode.

    ```python
        .writeStream
        .trigger(processingTime="3 seconds")
        .outputMode("append")
    ```

7. Now we specify the target for the output. This will write to the Pravega stream named `streamprocessing1`.

    ```python
        .format("pravega")
        .option("allow_create_scope", allowCreateScope)
        .option("controller", controller)
        .option("scope", scope)
        .option("stream", "streamprocessing1")
        .option("checkpointLocation", checkPointLocation)
    ```

8. Finally, we start the Spark job that we defined and wait for it to complete. Since this is an unbounded job, it will continue to run until stopped by the user.

    ```python
        .start()
        .awaitTermination()
    )
    ```

## Running the Application Locally

Follow these steps to run this application locally and write to your local development installation of Pravega.

1. Download [stream_generated_data_to_pravega.py](https://github.com/pravega/pravega-samples/blob/spark-connector-examples/spark-connector-examples/src/main/python/stream_generated_data_to_pravega.py) and save it to a file.

2. To run this application locally and write to your local development installation of Pravega, we'll use `spark-submit --master 'local[2]'`. This will start a Spark mini-cluster on your local system and use 2 CPUs.

    ```shell
    spark-submit \
      --master 'local[2]' \
      --driver-memory 4g \
      --executor-memory 4g \
      --total-executor-cores 1 \
      --packages io.pravega:pravega-connectors-spark-3.0_2.12:0.9.0 \
      stream_generated_data_to_pravega.py
    ```

   This job will continue to run and write events until stopped.

import DeployPythonSpark from '../snippets/spark-connectors/deploy-python-spark.md';

<DeployPythonSpark />
