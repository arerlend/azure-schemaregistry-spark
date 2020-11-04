package com.microsoft.azure

import java.io.ByteArrayInputStream

import com.azure.core.util.serializer.TypeReference
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder
import com.azure.data.schemaregistry.avro.SchemaRegistryAvroSerializerBuilder
import com.azure.identity.ClientSecretCredentialBuilder

import scala.util.control.NonFatal
import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, SpecificInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class AvroDataToCatalyst(
     child: Expression,
     options: Map[java.lang.String, java.lang.String])
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[BinaryType] = Seq(BinaryType)

  override lazy val dataType: DataType = StringType

  override def nullable: Boolean = true

  @transient private lazy val deserializer = new SchemaRegistryAvroSerializerBuilder()
      .schemaRegistryAsyncClient(new SchemaRegistryClientBuilder()
        .endpoint(options.getOrElse("schema.registry.url", null))
        .credential(new ClientSecretCredentialBuilder()
          .tenantId(options.getOrElse("schema.registry.tenant.id", null))
          .clientId(options.getOrElse("schema.registry.client.id", null))
          .clientSecret(options.getOrElse("schema.registry.client.secret", null))
          .build())
        .buildAsyncClient())
      .schemaGroup(options.getOrElse("schema.group", null))
      .autoRegisterSchema(options.getOrElse("specific.avro.reader", false).asInstanceOf[Boolean])
      .buildSerializer()

  @transient private lazy val parseMode: ParseMode = {
    FailFastMode
  }

  private def unacceptableModeMessage(name: String): String = {
    s"from_avro() doesn't support the $name mode. " +
      s"Acceptable modes are ${PermissiveMode.name} and ${FailFastMode.name}."
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
    val binary = new ByteArrayInputStream(input.asInstanceOf[Array[Byte]])
    val s = deserializer.deserialize(binary, TypeReference.createInstance(classOf[GenericRecord])).toString()
    UTF8String.fromString(s)
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