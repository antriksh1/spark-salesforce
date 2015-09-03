/*
 * Copyright 2015 springml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.springml.spark.salesforce

import com.sforce.soap.partner.sobject.SObject
import com.sforce.soap.partner.{Connector, PartnerConnection}
import com.sforce.ws.ConnectorConfig
import com.springml.spark.salesforce.Utils._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import com.springml.spark.salesforce.metadata.MetadataConstructor
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import com.springml.salesforce.wave.api.APIFactory

/**
 * Default source for SalesForce wave data source. It writes any
 * given DF to Salesforce wave repository
 *
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])
  private def createReturnRelation(data: DataFrame) = {

    new BaseRelation {
      override def sqlContext: SQLContext = data.sqlContext
      override def schema: StructType = data.schema
    }
  }

  /**
   * Execute the SAQL against Salesforce Wave and construct dataframe with the result
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   *
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    val username = param(parameters, "SF_USERNAME", "username")
    val password = param(parameters, "SF_PASSWORD", "password")
    val login = parameters.getOrElse("login", "https://login.salesforce.com")
    val version = parameters.getOrElse("version", "34.0")
    val saql = parameters.get("saql")
    val soql = parameters.get("soql")

    if ((saql.isDefined && soql.isDefined)) {
      sys.error("Anyone 'saql' or 'soql' have to be specified for creating dataframe")
    }

    if ((!saql.isDefined && !soql.isDefined)) {
      sys.error("Either 'saql' or 'soql' have to be specified for creating dataframe")
    }

    if (saql.isDefined) {
      val waveAPI = APIFactory.getInstance.waveAPI(username, password, login)
      DatasetRelation(waveAPI, null, saql.get, schema, sqlContext)
    } else {
      val forceAPI = APIFactory.getInstance.forceAPI(username, password, login)
      DatasetRelation(null, forceAPI, soql.get, schema, sqlContext)
    }

  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val username = param(parameters, "SF_USERNAME", "username")
    val password = param(parameters, "SF_PASSWORD", "password")
    val datasetName = parameters.getOrElse("datasetName", sys.error("'datasetName' must be specified for salesforce."))
    val login = parameters.getOrElse("login", "https://login.salesforce.com")
    val version = parameters.getOrElse("version", "34.0")
    val usersMetadataConfig = parameters.get("metadataConfig")

    val dataWriter = new DataWriter(username, password, login, version, datasetName)

    val metadataConfig = Utils.metadataConfig(usersMetadataConfig)
    val metaDataJson = MetadataConstructor.generateMetaString(data.schema, datasetName, metadataConfig)
    logger.info(s"Metadata for dataset $datasetName is $metaDataJson")
    logger.info("Uploading metadata for dataset " + datasetName)

    val writtenId = dataWriter.writeMetadata(metaDataJson, mode)
    if (!writtenId.isDefined) {
      sys.error("Unable to write metadata for dataset " + datasetName)
    }
    logger.info(s"Able to write the metadata is $writtenId")

    logger.info("no of partitions before repartitioning is " + data.rdd.partitions.length)
    logger.info("Repartitioning rdd for 10mb partitions")
    val repartitionedRDD = Utils.repartition(data.rdd)
    logger.info("no of partitions after repartitioning is " + repartitionedRDD.partitions.length)

    logger.info("Writing data")
    val successfulWrite = dataWriter.writeData(repartitionedRDD, writtenId.get)
    logger.info(s"Writing data was successful was $successfulWrite")
    if (!successfulWrite) {
      sys.error("Unable to write data for " + datasetName)
    }

    logger.info("Committing...")
    val committed = dataWriter.commit(writtenId.get)
    logger.info(s"committing data was successful was $committed")

    if (!committed) {
      sys.error("Unable to commit data for " + datasetName)
    }
    logger.info(s"Successfully written data for dataset $datasetName ")
    println(s"Successfully written data for dataset $datasetName ")

    return createReturnRelation(data)
  }

  private def param(parameters: Map[String, String], envName: String, paramName: String) : String = {
    val envProp = sys.env.get(envName);
    if (envProp != null && envProp.isDefined) {
      return envProp.get
    }

    parameters.getOrElse(paramName,
        sys.error(s"""Either '$envName' has to be added in environment or '$paramName' must be specified for salesforce package."""));
  }
}
