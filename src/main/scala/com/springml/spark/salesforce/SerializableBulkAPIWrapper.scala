package com.springml.spark.salesforce

import com.springml.salesforce.wave.api.{APIFactory, BulkAPI}

object SerializableBulkAPIWrapper extends Serializable {
  @transient private var bulkAPI: BulkAPI = _

  def initializeInstance(username: String,
                         password: String,
                         login: String,
                         version: String): Unit = {
    bulkAPI = APIFactory.getInstance().bulkAPI(username, password, login, version)
  }

  def getInstance: BulkAPI = bulkAPI
}
