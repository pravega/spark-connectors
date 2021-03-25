<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
-->
# Prepare Development Environment

## Prerequisites

- **Operating System**: Spark runs on both Windows and UNIX-like systems (e.g. Linux, Mac OS), and it should run on any platform that runs a supported version of Java. The steps below show the specific steps for Ubuntu 20.04 LTS. Similar steps can be executed on other operating systems.

- **Java 11**: Java 11 JDK is required. If you are using Ubuntu, you may use these steps to install it.

    ```shell
    sudo apt-get install openjdk-11-jdk
    ```

    You may have multiple versions of Java installed. Ensure that Java 11 is the default with the command below.

    ```shell
    sudo update-alternatives --config java
    ```

## Run Pravega

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

## Install Apache Spark

This will install a development instance of Spark locally. In the steps below, we show Spark version 3.0.2 but you should use the latest 3.0.x version of Spark that is available.

Download https://www.apache.org/dyn/closer.lua/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz.

```shell
mkdir -p ~/spark
cd ~/spark
tar -xzvf ~/Downloads/spark-3.0.2-bin-hadoop2.7.tgz
ln -snf spark-3.0.2-bin-hadoop2.7 current
export PATH="$HOME/spark/current/bin:$PATH"
```

## Clone Pravega Samples Repository

This will download the Pravega samples, which includes Spark samples written in Python and Scala. Cloning this repo is *not* required for Python Spark samples.

```shell
cd
git clone https://github.com/pravega/pravega-samples
cd pravega-samples
git checkout spark-connector-examples --
cd spark-connector-examples
```
