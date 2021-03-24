---
title: Limitations
---

<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

- This connector does *not* guarantee that events with the same routing key are returned in a single partition. If your application requires this, you must repartition the dataframe by the routing key and sort within the partition by segment_id and offset.

- Continuous reader support is not available. The micro-batch reader uses the Pravega batch API and works well for applications with latency requirements above 100 milliseconds.

- The initial batch in the micro-batch reader will contain the entire Pravega stream as of the start time. There is no rate limiting functionality.

- Read-after-write consistency is currently *not* guaranteed. Be cautious if your workflow requires multiple chained Spark batch jobs.
