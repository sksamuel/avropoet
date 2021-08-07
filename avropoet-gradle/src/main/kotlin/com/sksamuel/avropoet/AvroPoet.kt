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
      types.forEach { spec.addType(it) }
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
         Schema.Type.MAP -> TODO()
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
         .addStatement("var s = this * this")
         .addStatement("return s")
         .build()

      val companion = TypeSpec.companionObjectBuilder()
         .addFunction(decoder)
         .build()

      builder
         .primaryConstructor(constructor.build())
         .addType(companion)
         .build()
         .apply { types.add(this) }

      FunSpec.builder("encode")
         .receiver(ref)
         .returns(Int::class)
         .addStatement("var s = this * this")
         .addStatement("return s")
         .build()
         .apply { encoders.add(this) }

      return ref
   }
}

