---
title: Tutorial 2 - Reading from Pravega
---

<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
-->

In this tutorial, we'll use [stream_pravega_to_console.py](https://github.com/pravega/pravega-samples/blob/spark-connector-examples/spark-connector-examples/src/main/python/stream_pravega_to_console.py).

## Code Walkthrough

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

import CheckpointDir from '../snippets/spark-connectors/checkpoint-dir.md';

<CheckpointDir />

To run this application, refer to the steps in [Tutorial 1](tutorial-1-writing-to-pravega.md#running-the-application-locally).
