package io.pravega.connectors.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

class PravegaScan(options: CaseInsensitiveStringMap) extends Scan with Logging {

  override def readSchema(): StructType = PravegaReader.pravegaSchema

  /**
   * Creates a {@link Batch} to scan the data from this data source.
   *
   * This is used to read a Pravega stream as a dataframe in a batch job.
   *
   * Unbounded stream cuts (earliest, latest, unbounded) are bound only once.
   * Late binding is not available.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   *
   */
  override def toBatch(): Batch = {

    val caseInsensitiveParams = CaseInsensitiveMap(options.asScala.toMap)
    PravegaSourceProvider.validateOptions(caseInsensitiveParams)
    val clientConfig = PravegaSourceProvider.buildClientConfig(caseInsensitiveParams)
    val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
    val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)

    val startStreamCut = PravegaSourceProvider.getPravegaStreamCut(
      caseInsensitiveParams, PravegaSourceProvider.START_STREAM_CUT_OPTION_KEY, EarliestStreamCut)

    val endStreamCut = PravegaSourceProvider.getPravegaStreamCut(
      caseInsensitiveParams, PravegaSourceProvider.END_STREAM_CUT_OPTION_KEY, LatestStreamCut)

    log.info(s"toBatch: clientConfig=${clientConfig}, scopeName=${scopeName}, streamName=${streamName}"
      + s" startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}")

    PravegaSourceProvider.createStream(caseInsensitiveParams)

    new PravegaBatch(
      scopeName,
      streamName,
      clientConfig,
      caseInsensitiveParams,
      startStreamCut,
      endStreamCut)
  }


  /**
   * Creates a {@link MicroBatchStream} to read batches of data from this data source in a
   * streaming query.
   * This is used to read a Pravega stream as a datastream in a structured streaming job.
   *
   * The execution engine will create a micro-batch reader at the start of a streaming query,
   * alternate calls to setOffsetRange and planInputPartitions for each batch to process, and
   * then call stop() when the execution is complete. Note that a single query may have multiple
   * executions due to restart or failure recovery.
   *
   * @param checkpointLocation a path to Hadoop FS scratch space that can be used for failure
   *                           recovery. Readers for the same logical source in the same query
   *                           will be given the same checkpointLocation. The Pravega connector
   *                           does not need to implement this checkpointLocation since the spark
   *                           handles the Pravega streamcut through the Offset interface.
   */
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    val caseInsensitiveParams = CaseInsensitiveMap(options.asScala.toMap)
    PravegaSourceProvider.validateOptions(caseInsensitiveParams)

    val clientConfig = PravegaSourceProvider.buildClientConfig(caseInsensitiveParams)
    val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
    val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)

    val startStreamCut = PravegaSourceProvider.getPravegaStreamCut(
      caseInsensitiveParams, PravegaSourceProvider.START_STREAM_CUT_OPTION_KEY, LatestStreamCut)

    val endStreamCut = PravegaSourceProvider.getPravegaStreamCut(
      caseInsensitiveParams, PravegaSourceProvider.END_STREAM_CUT_OPTION_KEY, UnboundedStreamCut)

    log.info(s"toMicroBatchStream: clientConfig=${clientConfig}, scopeName=${scopeName}, streamName=${streamName}"
      + s" startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}")

    PravegaSourceProvider.createStream(caseInsensitiveParams)

    new PravegaMicroBatchStream(
      scopeName,
      streamName,
      clientConfig,
      caseInsensitiveParams,
      startStreamCut,
      endStreamCut)
  }

}
