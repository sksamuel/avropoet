package com.sksamuel.avropoet

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName
import com.squareup.kotlinpoet.asTypeName
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.nio.file.Files
import java.nio.file.Path

class AvroPoet {

   private val types = mutableListOf<TypeSpec>()
   private val encoders = mutableListOf<FunSpec>()

   fun generate(input: Path, outputBase: Path) {

      val schema = Schema.Parser().parse(input.toFile())
      record(schema)

      val spec = FileSpec.builder(schema.namespace, schema.name)
      spec.addImport(GenericData::class.java.`package`.name, "GenericData")
      types.distinctBy { it.name }.forEach { spec.addType(it) }
      encoders.forEach { spec.addFunction(it) }

      val outputPath = schema.namespace.split('.')
         .fold(outputBase) { acc, op -> acc.resolve(op) }
         .resolve(schema.name + ".kt")

      outputPath.parent.toFile().mkdirs()

      println("Writing to $outputBase")

      val contents = spec.build().toString()
      println("File contents $contents")
      Files.writeString(outputPath, contents)
   }

   private fun ref(schema: Schema): TypeName {
      return when (schema.type) {
         Schema.Type.RECORD -> record(schema)
         Schema.Type.ENUM -> TODO()
         Schema.Type.ARRAY -> ClassName("kotlin.collections", "List").parameterizedBy(ref(schema.elementType))
         Schema.Type.MAP -> ClassName("kotlin.collections", "Map").parameterizedBy(
            String::class.asClassName(),
            ref(schema.valueType)
         )
         Schema.Type.UNION -> TODO()
         Schema.Type.FIXED -> TODO()
         Schema.Type.STRING -> String::class.asTypeName()
         Schema.Type.BYTES -> ByteArray::class.asTypeName()
         Schema.Type.INT -> Int::class.asTypeName()
         Schema.Type.LONG -> Long::class.asTypeName()
         Schema.Type.FLOAT -> Float::class.asTypeName()
         Schema.Type.DOUBLE -> Double::class.asTypeName()
         Schema.Type.BOOLEAN -> Boolean::class.asTypeName()
         Schema.Type.NULL -> TODO()
         null -> error("Invalid code path")
      }
   }

   private fun type(schema: Schema): String {
      return when (schema.type) {
         Schema.Type.RECORD -> schema.name
         Schema.Type.ENUM -> schema.name
         Schema.Type.ARRAY -> "List<${type(schema.elementType)}>"
         Schema.Type.MAP -> "Map<String, ${type(schema.valueType)}>"
         Schema.Type.UNION -> TODO()
         Schema.Type.FIXED -> TODO()
         Schema.Type.STRING -> "String"
         Schema.Type.BYTES -> "ByteArray"
         Schema.Type.INT -> "Int"
         Schema.Type.LONG -> "Long"
         Schema.Type.FLOAT -> "Float"
         Schema.Type.DOUBLE -> "Double"
         Schema.Type.BOOLEAN -> "Boolean"
         Schema.Type.NULL -> TODO()
      }
   }

   private fun record(schema: Schema): ClassName {
      require(schema.type == Schema.Type.RECORD) { "$schema must be record" }

      val builder = TypeSpec.classBuilder(schema.name)
         .addModifiers(KModifier.DATA)

      val constructor = FunSpec.constructorBuilder()
      schema.fields.map { field ->
         val ref = ref(field.schema())
         constructor.addParameter(ParameterSpec.builder(field.name(), ref).build())
         builder.addProperty(PropertySpec.builder(field.name(), ref).initializer(field.name()).build())
      }

      val ref = ClassName(schema.namespace, schema.name)

      val decoder = FunSpec.builder("decode")
         .addParameter("record", GenericRecord::class.asClassName())
         .returns(ref)
         .addCode("return ${schema.name}(\n")
      schema.fields.forEach {
         decoder.addCode("\trecord.get(%S) as ${type(it.schema())},\n", it.name())
      }
      decoder.addCode(")")


      val companion = TypeSpec.companionObjectBuilder()
         .addFunction(decoder.build())
         .build()

      builder
         .primaryConstructor(constructor.build())
         .addType(companion)
         .build()
         .apply { types.add(this) }

      val encoder = FunSpec.builder("encode")
         .receiver(ref)
         .addParameter("schema", Schema::class.asClassName())
         .returns(GenericRecord::class.asClassName())
         .addStatement("val record = GenericData.Record(schema)")
      schema.fields.forEach {
         encoder.addStatement("record.put(%S, this.${it.name()})", it.name())
      }
      encoder.addStatement("return record")
         .build()
         .apply { encoders.add(this) }

      return ref
   }
}

