# azure-schemaregistry-spark (WIP)

## Overview

Schema Registry support in Java is provided by the official Schema Registry SDK in the Azure Java SDK repository.

Schema Registry serializer craft payloads that contain a schema ID and an encoded payload.  The ID references a registry-stored schema that can be used to decode the user-specified payload.

However, consuming Schema Registry-backed payloads in Spark is particularly difficult, since - 
- Spark Kafka does not support plug-in with KafkaSerializer and KafkaDeserializer objects, and
- Object management is non-trivial given Spark's driver-executor model.

For these reasons, Spark functions are required to simplify SR UX in Spark.  This repository contains packages that will provide Spark support in Scala for serialization and deserialization of registry-backed payloads.  Code is work in progress.

Currently, only Avro encodings are supported by Azure Schema Registry clients.  `from_avro` and `to_avro` found in the `functions.scala` files will be usable for converting Spark SQL columns from registry-backed payloads to columns of the correct Spark SQL datatype (e.g. `StringType`, `StructType`, etc.).

Spark/Databricks usage is the following:

```scala
     val props: HashMap[String, String] = new HashMap()
     props.put("schema.registry.url", SCHEMA_REGISTRY_URL)
     props.put("schema.registry.tenant.id", SCHEMA_REGISTRY_TENANT_ID)
     props.put("schema.registry.client.id", SCHEMA_REGISTRY_CLIENT_ID)
     props.put("schema.registry.client.secret", SCHEMA_REGISTRY_CLIENT_SECRET)
     

     val df = spark.readStream
          .format("kafka")
          .option("subscribe", TOPIC)
          .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
          .option("kafka.sasl.mechanism", "PLAIN")
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.jaas.config", EH_SASL)
          .option("kafka.request.timeout.ms", "60000")
          .option("kafka.session.timeout.ms", "60000")
          .option("failOnDataLoss", "false")
          .option("startingOffsets", "earliest")
          .option("kafka.group.id", "kafka-group")
          .load()

     df.select(from_avro($"value", "[schema guid]", props))  // path will be changed in the future
          .writeStream
          .outputMode("append")
          .format("console")
          .start()
          .awaitTermination()
```

See also:
- aka.ms/schemaregistry
- https://github.com/Azure/azure-schema-registry-for-kafka
