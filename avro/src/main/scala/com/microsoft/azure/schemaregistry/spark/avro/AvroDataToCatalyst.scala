package com.microsoft.azure.schemaregistry.spark.avro

import java.io.ByteArrayInputStream

import com.azure.core.util.serializer.TypeReference
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder
import com.azure.data.schemaregistry.avro.{SchemaRegistryAvroSerializer, SchemaRegistryAvroSerializerBuilder}
import com.azure.identity.ClientSecretCredentialBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, SpecificInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.types._

import scala.util.control.NonFatal

case class AvroDataToCatalyst(
     child: Expression,
     schemaId: String,
     options: Map[java.lang.String, java.lang.String],
     requireExactSchemaMatch: Boolean)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[BinaryType] = Seq(BinaryType)

  override lazy val dataType: DataType = {
    val dt = SchemaConverters.toSqlType(new Schema.Parser().parse(expectedSchemaString)).dataType;
    dt
    // todo: compare stream compat to group compat and verify equal
  }

  override def nullable: Boolean = true

  private val expectedSchemaString : String = {
    new String(schemaRegistryAsyncClient.getSchema(schemaId).block().getSchema)
  }

  @transient private lazy val schemaRegistryCredential = new ClientSecretCredentialBuilder()
    .tenantId(options.getOrElse("schema.registry.tenant.id", null))
    .clientId(options.getOrElse("schema.registry.client.id", null))
    .clientSecret(options.getOrElse("schema.registry.client.secret", null))
    .build()

  @transient private lazy val schemaRegistryAsyncClient = new SchemaRegistryClientBuilder()
    .endpoint(options.getOrElse("schema.registry.url", null))
    .credential(schemaRegistryCredential)
    .buildAsyncClient()

  @transient private lazy val deserializer =  new SchemaRegistryAvroSerializerBuilder()
      .schemaRegistryAsyncClient(schemaRegistryAsyncClient)
      .schemaGroup(options.getOrElse("schema.group", null))
      .autoRegisterSchema(options.getOrElse("specific.avro.reader", false).asInstanceOf[Boolean])
      .buildSerializer()

  @transient private lazy val avroConverter = {
    new AvroDeserializer(new Schema.Parser().parse(expectedSchemaString), dataType)
  }

  @transient private lazy val expectedSchema = new Schema.Parser().parse(expectedSchemaString)

  @transient private lazy val parseMode: ParseMode = {
    FailFastMode // permissive mode
  }

  @transient private lazy val nullResultRow: Any = dataType match {
    case st: StructType =>
      val resultRow = new SpecificInternalRow(st.map(_.dataType))
      for(i <- 0 until st.length) {
        resultRow.setNullAt(i)
      }
      resultRow

    case _ =>
      null
  }

  override def nullSafeEval(input: Any): Any = {
    try {
      val binary = new ByteArrayInputStream(input.asInstanceOf[Array[Byte]])
      // compare schema version and datatype version
      val genericRecord = deserializer.deserialize(binary, TypeReference.createInstance(classOf[GenericRecord]))

      if (requireExactSchemaMatch) {
        if (!expectedSchema.equals(genericRecord.getSchema)) {
          throw new IncompatibleSchemaException(s"Schema not exact match, payload schema did not match expected schema.  Payload schema: ${genericRecord.getSchema}")
        }
      }

      avroConverter.deserialize(genericRecord).get
    } catch {
      case NonFatal(e) => parseMode match {
        case PermissiveMode => nullResultRow
        case FailFastMode =>
          throw new Exception("Malformed records are detected in record parsing. " +
            s"Current parse Mode: ${FailFastMode.name}. To process malformed records as null " +
            "result, try setting the option 'mode' as 'PERMISSIVE'.", e)
        case _ =>
          throw new Exception(s"Unknown parse mode: ${parseMode.name}")
      }
    }
  }

  override def prettyName: String = "from_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, eval => {
      val result = ctx.freshName("result")
      val dt = CodeGenerator.boxedType(dataType)
      s"""
        $dt $result = ($dt) $expr.nullSafeEval($eval);
        if ($result == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $result;
        }
      """
    })
  }
}