## Spark functions support for Schema Registry (Scala)

Schema Registry support in Java is provided by the official Schema Registry SDK in the Azure Java SDK repository.

Schema Registry serializer craft payloads that contain a schema ID and an encoded payload.  The ID references a registry-stored schema that can be used to decode the user-specified payload.

However, consuming Schema Registry-backed 