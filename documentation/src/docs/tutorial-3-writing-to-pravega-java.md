---
title: Tutorial 3 - Writing to Pravega using Java
---

<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
-->

In this tutorial, we will create a Java version of `stream_generated_data_to_pravega.py` which was described in [Tutorial 1](tutorial-1-writing-to-pravega.md).

## Prerequisites

- **Java 11**: Java 11 JDK is required. Refer to [Prepare Development Environment](prepare-development-environment.md#prerequisites).

- **Gradle**: This tutorial uses [Gradle](https://gradle.org/) to build the Java package. If you are using Ubuntu, you may use this command to install it.

    ```shell
    sudo apt-get install gradle
    ```

## Code Walkthrough

1. Create the main Java class file `src/main/java/GeneratedDataToPravega.java`.

    ```java title="src/main/java/GeneratedDataToPravega.java"
    import org.apache.spark.sql.SparkSession;
    import org.apache.spark.sql.Dataset;
    import org.apache.spark.sql.streaming.Trigger;

    public class GeneratedDataToPravega {
        public static void main(String[] args) throws Exception {
            SparkSession spark = SparkSession.builder().getOrCreate();
            String scope = System.getenv().getOrDefault("PRAVEGA_SCOPE", "examples");
            boolean allowCreateScope = !System.getenv().containsKey("PROJECT_NAME");
            String controller = System.getenv().getOrDefault("PRAVEGA_CONTROLLER_URI", "tcp://127.0.0.1:9090");
            String checkpointLocation = System.getenv().getOrDefault("CHECKPOINT_DIR", "/tmp/spark-checkpoints-GeneratedDataToPravega");

            spark
                .readStream()
                .format("rate")
                .load()
                .selectExpr("cast(timestamp as string) as event", "cast(value as string) as routing_key")
                .writeStream()
                .trigger(Trigger.ProcessingTime("3 seconds"))
                .outputMode("append")
                .format("pravega")
                .option("allow_create_scope", allowCreateScope)
                .option("controller", controller)
                .option("scope", scope)
                .option("stream", "streamprocessing1")
                .option("checkpointLocation", checkpointLocation)
                .start()
                .awaitTermination();
        }
    }
    ```

2. Create `build.gradle`.

    ```groovy title="build.gradle"
    apply plugin: "java"

    sourceCompatibility = "1.8"
    targetCompatibility = "1.8"
    archivesBaseName = "my-spark-app"

    repositories {
        mavenLocal()
        mavenCentral()
    }

    dependencies {
        compileOnly group: "org.apache.spark", name: "spark-sql_2.12", version: "3.0.1"
    }
    ```

3. Compile your Java source code to produce a JAR file.

   This will build the file `build/libs/my-spark-app.jar`.

   ```shell
   gradle build
   ```

## Running the Application Locally

Follow these steps to run this application locally and write to your local development installation of Pravega.

1. Run spark-submit.

    ```shell
    spark-submit \
      --master 'local[2]' \
      --driver-memory 4g \
      --executor-memory 4g \
      --total-executor-cores 1 \
      --packages io.pravega:pravega-connectors-spark-3.0_2.12:0.9.0 \
      --class GeneratedDataToPravega \
      build/libs/my-spark-app.jar
    ```

   This job will continue to run and write events until stopped.

import DeployJavaSpark from '../snippets/spark-connectors/deploy-java-spark.md';

<DeployJavaSpark />
