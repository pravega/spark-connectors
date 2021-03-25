<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Building the Connector

You only need to build the connector if you need to customize it.

## Prerequisites

The following prerequisites are required for building this connector:

- Java 8 or 11 (see compatibility matrix)

## Build and Install the Pravega Spark Connectors

This will build the Pravega Spark Connectors and publish it to your local Maven repository.

```shell
git clone https://github.com/pravega/spark-connectors
cd spark-connectors
./gradlew install
ls -lhR ~/.m2/repository/io/pravega/
```
