---
title: Configuration
---

<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

## Connector Parameters

The following table lists the configurable parameters of the Pravega Spark connector and their default values.

| Parameter | Description | Default |
| ----- | ----------- | ------ |
| `allow_create_scope` | If true, the Pravega scope will be automatically created. This must be false when running in Dell EMC Streaming Data Platform. | `true` |
| `allow_create_stream` | If true, the Pravega stream will be automatically created. | `true` |
| `controller` |  The URI endpoint of the Pravega controller in the form of `protocol://hostname:port`.| `tcp://localhost:9090` |
| `default_num_segments` | The default number of segments for a stream. This is ignored if the stream already exists. | |
| `default_retention_duration_milliseconds` | The default time in form of `ms` to decide how much data to retain within a stream. This is ignored if the stream already exists. |  |
| `default_retention_size_bytes` | The default size in the form of `bytes` to decide how much data to retain within a stream. This is ignored if the stream already exists.  | |
| `default_scale_factor` | The default scale factor for a stream to decide if it should automatically scale its number of segments. This is ignored if the stream already exists.  |  |
| `default_segment_target_rate_bytes_per_sec` | The target rate for a segment in the form of `bytes` per second. This is ignored if the stream already exists. | |
| `default_segment_target_rate_events_per_sec` | The target rate for a segment in the form of `events` per second. This is ignored if the stream already exists. | |
| `end_stream_cut` | The end stream cut (offsets) of a stream. Can be `latest`, `unbounded`, or a specific base-64 encoded stream cut. `latest` will be resolved at the start of the job. | Batch Job: `latest`; Stream Job: `unbounded` |
| `exactly_once` | If true, use Pravega transactions when writing to provide exactly-once semantics. Set to false for reduced write latency. | `true` |
| `metadata` | If set, a read will return scope or stream metadata instead of the stream data. "Streams" will return a list of streams in the scope. "StreamInfo" will return head and tail stream cuts.| |
| `read_after_write_consistency` | If true, commits will wait for Pravega to finish committing transactions before completing. | `true` |
| `scope` | The Pravega scope containing the data stream. |  |
| `start_stream_cut` | The start stream cut (offsets) of a stream. Can be `earliest`, `latest`, `unbounded`, or a specific base-64 encoded stream cut. `earliest` and `latest` will be resolved at the start of the job. | Batch Job: `earliest`; Stream Job: `latest` |
| `stream` | The name of the Pravega stream to read or write. |  |
| `transaction_timeout_ms` | The time-out value for a transaction in the form of `ms`. | Batch Job: `120000` (2 minutes); Stream Job: `30000` |
| `transaction_status_poll_interval_ms` |  The time interval in `ms` for which the transaction status is polled. This is used only if `read_after_write_consistency` is true. | `50` |

## Configuring Pravega for Exactly-Once

When writing events to Pravega with `exactly_once` set to true (the default), Pravega transactions are used.
This connector begins a transaction during the execution of each task and commits all transactions
only at the end of the batch job or at the end of the micro-batch.
To prevent the Pravega transaction from timing out, you must apply the following configuration.

1. The Pravega Controller must be configured with the following settings to allow transactions to remain open for up to 30 days without lease renewals.
    ```
    -Dcontroller.transaction.lease.count.max=2592000000
    -Dcontroller.transaction.execution.timeBound.days=30
    ```

2. The Pravega Spark connector writer must be configured with a reasonable value for `transaction_timeout_ms`.
   This should be the maximum number of milliseconds that you expect the Spark job to take.
   It must not exceed `controller.transaction.lease.count.max`.
