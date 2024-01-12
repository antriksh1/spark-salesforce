package com.springml.spark.salesforce

import java.text.SimpleDateFormat

import com.springml.salesforce.wave.api.{APIFactory, BulkAPI}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.springml.salesforce.wave.model.{BatchInfo, BatchInfoList, JobInfo}
import org.apache.http.Header
import org.apache.spark.rdd.RDD
import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import com.univocity.parsers.csv.CsvWriterSettings
import com.univocity.parsers.csv.CsvWriter
import java.io.StringReader
import java.io.StringWriter
import scala.collection.mutable.ListBuffer

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/**
  * Relation class for reading data from Salesforce and construct RDD
  */
case class BulkRelation(
    username: String,
    password: String,
    login: String,
    version: String,
    query: String,
    sfObject: String,
    customHeaders: List[Header],
    userSchema: StructType,
    sqlContext: SQLContext,
    inferSchema: Boolean,
    timeout: Long,
    maxCharsPerColumn: Int,
    parameters: Map[String, String]) extends BaseRelation with TableScan {

  import sqlContext.sparkSession.implicits._
  import scala.collection.JavaConversions._

  @transient lazy val logger: Logger = Logger.getLogger(classOf[BulkRelation])

  def buildScan() = records.rdd

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      records.schema
    }

  }

  lazy val records: DataFrame = {
    val multiQueryStr = parameters.getOrElse("multiQuery", "false")
    if(multiQueryStr == "true") {
      logger.error("Will be processing with MULTIPLE Queries")
      multiQueryRecords
    } else {
      logger.error("Will be processing with SINGLE Query")
      singleQueryRecords
    }
  }

  lazy val singleQueryRecords: DataFrame = {
    val inputJobInfo = new JobInfo("CSV", sfObject, "queryAll")

    if(bulkAPI == null) {
      SerializableBulkAPIWrapper.initializeInstance(username, password, login, version)
    }

    val jobInfo = bulkAPI.createJob(inputJobInfo, customHeaders.asJava)
    val jobId = jobInfo.getId
    logger.error(">>> Obtained jobId: " + jobId)

    val batchInfo = bulkAPI.addBatch(jobId, query)
    logger.error(">>> Obtained batchInfo: " + batchInfo)

    if (awaitJobCompleted(jobId)) {
      bulkAPI.closeJob(jobId)

      val batchInfoList = bulkAPI.getBatchInfoList(jobId)
      val batchInfos = batchInfoList.getBatchInfo().asScala.toList

      logger.error(">>> Obtained batchInfos: " + batchInfos)
      logger.error(">>>>>> Obtained batchInfos.size: " + batchInfos.size)

      val completedBatchInfos = batchInfos.filter(batchInfo => batchInfo.getState().equals("Completed"))
      logger.error(">>> Obtained completedBatchInfos.size ALL : " + completedBatchInfos.size)

      //val completedBatchInfoIds = completedBatchInfos.map(batchInfo => batchInfo.getId)
      // AS ddition 12/04
      val completedBatchInfoIds = completedBatchInfos
        .filter(batchInfo =>  batchInfo.getNumberRecordsProcessed > 0)
        .map(batchInfo => batchInfo.getId)

      logger.error(">>> Obtained completedBatchInfoIds WITH RECORDS: " + completedBatchInfoIds)
      logger.error(">>> Obtained completedBatchInfoIds.size WITH RECORDS: " + completedBatchInfoIds.size)

      completedBatchInfos
        .filter(batchInfo =>  batchInfo.getNumberRecordsProcessed > 0)
        .foreach(batchInfo => {
          logger.error(">>> Obtained completedBatchInfoId: " +  batchInfo.getId + ", WITH Records Processed: " + batchInfo.getNumberRecordsProcessed)
        })

      def splitCsvByRows(csvString: String): Seq[String] = {
        // The CsvParser interface only interacts with IO, so StringReader and StringWriter
        val inputReader = new StringReader(csvString)

        val parserSettings = new CsvParserSettings()
        parserSettings.setMaxColumns(1024)
        parserSettings.setLineSeparatorDetectionEnabled(true)
        parserSettings.getFormat.setNormalizedNewline(' ')
        parserSettings.setMaxCharsPerColumn(maxCharsPerColumn)

        val readerParser = new CsvParser(parserSettings)
        val parsedInput = readerParser.parseAll(inputReader).asScala

        val outputWriter = new StringWriter()

        val writerSettings = new CsvWriterSettings()
        writerSettings.setQuoteAllFields(true)
        writerSettings.setQuoteEscapingEnabled(true)

        val writer = new CsvWriter(outputWriter, writerSettings)
        parsedInput.foreach {
          writer.writeRow(_)
        }

//        outputWriter.toString.lines.toList
        outputWriter.toString.split("\\r?\\n").toList
      }

      val fetchAllResults = (resultId: String, batchInfoId: String) => {
        logger.error("About to Result for ResultId: " + resultId)

        if(bulkAPI == null) {
          SerializableBulkAPIWrapper.initializeInstance(username, password, login, version)
        }

        val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultId)

        val splitRows = splitCsvByRows(result)

        logger.error("Result Rows size: " + splitRows.size)
        logger.error("Result Row - first: " + (if (splitRows.size > 0) splitRows.head else "not found"))

        splitRows
      }

      val fetchBatchInfo = (batchInfoId: String) => {
        logger.error(">>> About to fetch Results in batchInfoId: " + batchInfoId)

        if (bulkAPI == null) {
          SerializableBulkAPIWrapper.initializeInstance(username, password, login, version)
        }

        val resultIds = bulkAPI.getBatchResultIds(jobId, batchInfoId)
        logger.error(">>> Got ResultsIds in batchInfoId: " + resultIds)
        logger.error(">>> Got ResultsIds in batchInfoId.size: " + resultIds.size)
        logger.error(">>> Got ResultsIds in Last Result Id: " + resultIds.get(resultIds.size() - 1))

//        val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultIds.get(resultIds.size() - 1))

//        logger.error(">>> Got Results - Results (string) length: " + result.length)

        // Use Csv parser to split CSV by rows to cover edge cases (ex. escaped characters, new line within string, etc)
//        def splitCsvByRows(csvString: String): Seq[String] = {
//          // The CsvParser interface only interacts with IO, so StringReader and StringWriter
//          val inputReader = new StringReader(csvString)
//
//          val parserSettings = new CsvParserSettings()
//          parserSettings.setLineSeparatorDetectionEnabled(true)
//          parserSettings.getFormat.setNormalizedNewline(' ')
//          parserSettings.setMaxCharsPerColumn(maxCharsPerColumn)
//
//          val readerParser = new CsvParser(parserSettings)
//          val parsedInput = readerParser.parseAll(inputReader).asScala
//
//          val outputWriter = new StringWriter()
//
//          val writerSettings = new CsvWriterSettings()
//          writerSettings.setQuoteAllFields(true)
//          writerSettings.setQuoteEscapingEnabled(true)
//
//          val writer = new CsvWriter(outputWriter, writerSettings)
//          parsedInput.foreach { writer.writeRow(_) }
//
//          outputWriter.toString.lines.toList
//        }

        val resultIdsBatchInfoIdPairs: List[(String, String)] = resultIds.toList.map { resultId: String => {
          (resultId, batchInfoId)
        }}

        // AS addition - START
//        val allRows: Seq[String] = resultIds.toList.flatMap { resultId: String => {
//          logger.error("Getting Result for ResultId: " + resultId)
//          val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultId)
//
//          val splitRows = splitCsvByRows(result)
//
//          logger.error("Result Rows size: " + splitRows.size)
//          logger.error("Result Row - first: " + (if (splitRows.size > 0) splitRows.head else "not found"))
//
//          splitRows
//        }}

        val allRows: Seq[String] = resultIdsBatchInfoIdPairs.flatMap { case(resultId, batchInfoId) =>
            fetchAllResults(resultId, batchInfoId)
        }

        allRows
        // AS Addition - END

//        val splitRows = splitCsvByRows(result)
//        logger.error("Result Rows size: " + splitRows.size)
//        logger.error("Result Row - first: " + (if (splitRows.size > 0) splitRows.head else "not found"))
//        splitRows

      }

      // AS addition - START
      val csvData: Dataset[String] = if (completedBatchInfoIds.size == 1) {
        val resultIds = bulkAPI.getBatchResultIds(jobId, completedBatchInfoIds.head)

        val resultIdsCompletedBatchInfoIdPairs: List[(String, String)] = resultIds.toList.map { resultId: String => {
          (resultId, completedBatchInfoIds.head)
        }}

        logger.error(">>>> Will Parallelize Result IDs, CBatchInfoId: " + resultIdsCompletedBatchInfoIdPairs)

        sqlContext
          .sparkContext
          .parallelize(resultIdsCompletedBatchInfoIdPairs)
          .flatMap { case (resultId, batchInfoId) =>
            fetchAllResults(resultId, batchInfoId)
          }.toDS()
      } else {
        logger.error(">>>> Will Parallelize CompletedBatchInfoIds: " + completedBatchInfoIds)

        sqlContext
          .sparkContext
          .parallelize(completedBatchInfoIds)
          .flatMap(fetchBatchInfo).toDS()
      }
      // AS addition - END

//      val csvData = sqlContext
//        .sparkContext
//        .parallelize(completedBatchInfoIds)
//        .flatMap(fetchBatchInfo).toDS()

      sqlContext
        .sparkSession
        .read
        .option("header", true)
        .option("inferSchema", inferSchema)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", true)
        .csv(csvData)
    } else {
      bulkAPI.closeJob(jobId)
      throw new Exception("Job completion timeout")
    }
  }

  lazy val multiQueryRecords: DataFrame = {
    val inputJobInfo = new JobInfo("CSV", sfObject, "queryAll")

    if (bulkAPI == null) {
      SerializableBulkAPIWrapper.initializeInstance(username, password, login, version)
    }

    val queries: List[String] = makeAllQueries(query, parameters)
    logger.error(">>>> MULTI-QUERY :: queries.length " + queries.length)
    for (query <- queries) {
      logger.error("MULTI-QUERY :: query >>>> " + query)
    }

    val jobIdsBuffer = ListBuffer[String]()

    for (query <- queries) {
      val jobInfo = bulkAPI.createJob(inputJobInfo, customHeaders.asJava)
      val jobId = jobInfo.getId
      jobIdsBuffer += jobId
      logger.error(">>> MULTI-QUERY :: Obtained jobId: " + jobId)

      val batchInfo = bulkAPI.addBatch(jobId, query)
      logger.error(">>> MULTI-QUERY :: Added/Submitted Batch, Obtained batchInfo: " + batchInfo)
    }

    val jobIds:List[String] = jobIdsBuffer.toList
    logger.error(">>>> MULTI-QUERY :: jobIds.length " + jobIds.length + ", jobIds: " + jobIds)

    if (awaitJobsCompleted(jobIds)) {
      def splitCsvByRows(csvString: String): Seq[String] = {
        // The CsvParser interface only interacts with IO, so StringReader and StringWriter
        val inputReader = new StringReader(csvString)

        val parserSettings = new CsvParserSettings()
        parserSettings.setMaxColumns(1024)
        parserSettings.setLineSeparatorDetectionEnabled(true)
        parserSettings.getFormat.setNormalizedNewline(' ')
        parserSettings.setMaxCharsPerColumn(maxCharsPerColumn)

        val readerParser = new CsvParser(parserSettings)
        val parsedInput = readerParser.parseAll(inputReader).asScala

        val outputWriter = new StringWriter()

        val writerSettings = new CsvWriterSettings()
        writerSettings.setQuoteAllFields(true)
        writerSettings.setQuoteEscapingEnabled(true)

        val writer = new CsvWriter(outputWriter, writerSettings)
        parsedInput.foreach {
          writer.writeRow(_)
        }

        //        outputWriter.toString.lines.toList
        outputWriter.toString.split("\\r?\\n").toList
      }

      val jobIdsCompletedBatchInfoIdPairsBuffer:ListBuffer[(String, String)] = ListBuffer[(String, String)]()

      for(jobId <- jobIds) {
        logger.error(">>> MULTI-QUERY :: Processing JobId: " + jobId)
        bulkAPI.closeJob(jobId)

        val batchInfoList = bulkAPI.getBatchInfoList(jobId)
        val batchInfos = batchInfoList.getBatchInfo().asScala.toList

        logger.error(">>> MULTI-QUERY :: Obtained batchInfos: " + batchInfos)
        logger.error(">>>>>> MULTI-QUERY :: Obtained batchInfos.size: " + batchInfos.size)

        val completedBatchInfos = batchInfos.filter(batchInfo => batchInfo.getState().equals("Completed"))
        logger.error(">>> MULTI-QUERY :: Obtained completedBatchInfos.size ALL : " + completedBatchInfos.size)

        val completedBatchInfoIds = completedBatchInfos
          .filter(batchInfo => batchInfo.getNumberRecordsProcessed > 0)
          .map(batchInfo => batchInfo.getId)

        logger.error(">>> MULTI-QUERY :: Obtained completedBatchInfoIds WITH >0 RECORDS: " + completedBatchInfoIds)
        logger.error(">>> MULTI-QUERY ::  Obtained completedBatchInfoIds.size WITH >0 RECORDS: " + completedBatchInfoIds.size)

        completedBatchInfos
          .filter(batchInfo => batchInfo.getNumberRecordsProcessed > 0)
          .foreach(batchInfo => {
            logger.error(">>> MULTI-QUERY :: Obtained completedBatchInfoId: " + batchInfo.getId + ", WITH Records Processed: " + batchInfo.getNumberRecordsProcessed)
          })

        val jobIdCompletedBatchInfoIdPairs = completedBatchInfoIds.map(completedBatchInfoId => (jobId, completedBatchInfoId))
        jobIdCompletedBatchInfoIdPairs.foreach { case(j, b) => logger.error( s"Adding JobID: $j, CompletedBatchId: $b") }
        jobIdsCompletedBatchInfoIdPairsBuffer ++= jobIdCompletedBatchInfoIdPairs
      }

      val jobIdsCompletedBatchInfoIdPairs = jobIdsCompletedBatchInfoIdPairsBuffer.toList
      logger.error(">>> MULTI-QUERY :: jobIdsCompletedBatchInfoIdPairs.length: " + jobIdsCompletedBatchInfoIdPairs.length)

      val fetchAllResults = (resultId: String, batchInfoId: String, jobId: String) => {
        logger.error(s"About to get Result for ResultId: $resultId, BatchId: $batchInfoId, JobId: $jobId")

        if (bulkAPI == null) {
          SerializableBulkAPIWrapper.initializeInstance(username, password, login, version)
        }

        val result = bulkAPI.getBatchResult(jobId, batchInfoId, resultId)

        val splitRows = splitCsvByRows(result)

        logger.error("Result Rows size: " + splitRows.size)
        logger.error("Result Row - first: " + (if (splitRows.size > 0) splitRows.head else "not found"))

        splitRows
      }

      val fetchBatches = (jobId: String, batchInfoId: String) => {
        logger.error(s">>> MULTI-QUERY :: About to get Results for: BatchId: $batchInfoId, JobId: $jobId")

        if (bulkAPI == null) {
          SerializableBulkAPIWrapper.initializeInstance(username, password, login, version)
        }

        val resultIds = bulkAPI.getBatchResultIds(jobId, batchInfoId)
        logger.error(s">>> For jobId $jobId, batchInfoId: $batchInfoId >> Got ResultsIds $resultIds")
        logger.error(s">>> For jobId $jobId, batchInfoId: $batchInfoId >> Got ResultsIds.size: "+ resultIds.size)
        logger.error(">>> Got ResultsIds - First Result Id: " + resultIds.get(0))
        logger.error(">>> Got ResultsIds - Last Result Id: " + resultIds.get(resultIds.size() - 1))

        val resultIdsBatchInfoIdPairs: List[(String, String)] = resultIds.toList.map { resultId: String => {
          (resultId, batchInfoId)
          }
        }

        val allRows: Seq[String] = resultIdsBatchInfoIdPairs.flatMap { case (resultId, batchInfoId) =>
          fetchAllResults(resultId, batchInfoId, jobId)
        }

        allRows
      }

      // Combine all results
      logger.error(">>>> MULTI-QUERY :: Will Parallelize jobIdCompletedBatchInfoIdPairs: " + jobIdsCompletedBatchInfoIdPairs)
      val csvData: Dataset[String] =
        sqlContext
          .sparkContext
          .parallelize(jobIdsCompletedBatchInfoIdPairs)
          .flatMap { case (jobId, batchInfoId) =>
            fetchBatches(jobId, batchInfoId)
          }.toDS()

      sqlContext
        .sparkSession
        .read
        .option("header", true)
        .option("inferSchema", inferSchema)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", true)
        .csv(csvData)
    } else {
      for(jobId <- jobIds) {
        bulkAPI.closeJob(jobId)
      }
      throw new Exception("Job completion timeout")
    }
  }

  // Create new instance of BulkAPI every time because Spark workers cannot serialize the object
  private def bulkAPI(): BulkAPI = {
//    APIFactory.getInstance().bulkAPI(username, password, login, version
    SerializableBulkAPIWrapper.getInstance
  }

  private def makeAllQueries(query: String, parameters: Map[String, String]): List[String] = {
    val theQuery = if(query.endsWith(";")) query.substring(0, query.length-1) else query

    val splitMonthlyStr = parameters.getOrElse("splitMonthly", "false")
    val splitWeeklyStr = parameters.getOrElse("splitWeekly", "false")
    val splitDailyStr = parameters.getOrElse("splitDaily", "false")

    val splitHourlyTooStr = parameters.getOrElse("splitHourlyToo", "false")

    val allQueriesBuffer = ListBuffer[String]()

    if(splitMonthlyStr == "true") {
      for (month <- 1 to 12) {
        val monthQuery = theQuery + s" AND CALENDAR_MONTH(CreatedDate)=$month"
        if(splitHourlyTooStr == "true") {
          for (hour <- 0 to 24) {
            val hourQuery = monthQuery + s" AND HOUR_IN_DAY(CreatedDate)=$hour"
            allQueriesBuffer += hourQuery
          }
        } else {
          allQueriesBuffer += monthQuery
        }
      }
    } else if (splitWeeklyStr == "true") {
      for (week <- 1 to 53) { // account for leap-year
        val weekQuery = theQuery + s" AND WEEK_IN_YEAR(CreatedDate)=$week"
        if (splitHourlyTooStr == "true") {
          for (hour <- 0 to 24) {
            val hourQuery = weekQuery + s" AND HOUR_IN_DAY(CreatedDate)=$hour"
            allQueriesBuffer += hourQuery
          }
        } else {
          allQueriesBuffer += weekQuery
        }
      }
    } else if (splitDailyStr == "true") {
      for (day <- 1 to 366) {
        val dayQuery = theQuery + s" AND DAY_IN_YEAR(CreatedDate)=$day"
        if (splitHourlyTooStr == "true") {
          for (hour <- 0 to 24) {
            val hourQuery = dayQuery + s" AND HOUR_IN_DAY(CreatedDate)=$hour"
            allQueriesBuffer += hourQuery
          }
        } else {
          allQueriesBuffer += dayQuery
        }
      }
    }

    allQueriesBuffer.toList
  }

  private def awaitJobCompleted(jobId: String): Boolean = {
    val timeoutDuration = FiniteDuration(timeout, MILLISECONDS)
    val initSleepIntervalDuration = FiniteDuration(200L, MILLISECONDS)
    val maxSleepIntervalDuration = FiniteDuration(10000L, MILLISECONDS)
    var completed = false
    Utils.retryWithExponentialBackoff(() => {
      completed = bulkAPI.isCompleted(jobId)
      completed
    }, timeoutDuration, initSleepIntervalDuration, maxSleepIntervalDuration)

    completed
  }

  private def awaitJobsCompleted(jobIds: List[String]): Boolean = {
    val timeoutDuration = FiniteDuration(timeout, MILLISECONDS)
    val initSleepIntervalDuration = FiniteDuration(200L, MILLISECONDS)
    val maxSleepIntervalDuration = FiniteDuration(10000L, MILLISECONDS)

    var statusesBuffer = ListBuffer[(String, Boolean)]()

    Utils.retryWithExponentialBackoff(() => {
      statusesBuffer = ListBuffer[(String, Boolean)]()

      for(jobId <- jobIds) {
        val completed = bulkAPI.isCompleted(jobId)
        val newTuple = (jobId, completed)
        statusesBuffer += newTuple
      }
      logger.error("Current Iteration Statuses: " + statusesBuffer)

      val statusesIn = statusesBuffer.toList
      !statusesIn.exists { case (_, completed) => !completed }
    }, timeoutDuration, initSleepIntervalDuration, maxSleepIntervalDuration)

    logger.error("Completion Statuses: " + statusesBuffer)
    val allCompleted = statusesBuffer.toList.forall { case (_, completed) => completed }

    allCompleted
  }

}