package com.microsoft.azure

import com.azure.data.schemaregistry.avro.{SchemaRegistryAvroSerializer}


import scala.collection.JavaConverters._

import org.apache.spark.sql.Column

object functions {
  var serializer: SchemaRegistryAvroSerializer = null

  def from_avro(
       data: Column,
       schemaId: String, // guid
       options: java.util.Map[java.lang.String, java.lang.String]): Column = {
    new Column(AvroDataToCatalyst(data.expr, schemaId, options.asScala.toMap))
  }

  def to_avro(data: Column, props : Map[String, AnyRef]): Column = {
    data
  }
}
