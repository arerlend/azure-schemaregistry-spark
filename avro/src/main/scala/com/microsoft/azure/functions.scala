package com.microsoft.azure.schemaregistry.spark

import com.azure.data.schemaregistry.avro.{SchemaRegistryAvroSerializer}


import scala.collection.JavaConverters._

import org.apache.spark.sql.Column

/***
 * Scala object containing utility methods for serialization/deserialization with Azure Schema Registry and Spark SQL
 * columns.
 *
 * Functions are agnostic to data source or sink and can be used with any Schema Registry payloads, including:
 * - Kafka Spark connector ($value)
 * - Event Hubs Spark connector ($Body)
 * - Event Hubs Avro Capture blobs ($Body)
 */
object functions {
  var serializer: SchemaRegistryAvroSerializer = null

  /***
   * Converts Spark SQL Column containing SR payloads into a
   * @param data column with SR payloads
   * @param schemaId GUID of the expected schema
   * @param clientOptions map of configuration properties, including Spark run mode (permissive vs. fail-fast)
   * @param requireExactSchemaMatch boolean if call should throw if data contents do not exactly match expected schema
   * @return
   */
  def from_avro(
       data: Column,
       schemaId: String,
       clientOptions: java.util.Map[java.lang.String, java.lang.String],
       requireExactSchemaMatch: Boolean = true): Column = {
    new Column(AvroDataToCatalyst(data.expr, schemaId, clientOptions.asScala.toMap, requireExactSchemaMatch))
  }

  def to_avro(data: Column, props : Map[String, AnyRef]): Column = {
    data
  }
}
