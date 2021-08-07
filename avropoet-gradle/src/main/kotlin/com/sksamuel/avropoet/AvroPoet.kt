package com.sksamuel.avropoet

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeSpec
import org.apache.avro.Schema
import java.nio.file.Files
import java.nio.file.Path

class AvroPoet {

   private val types = mutableListOf<TypeSpec>()

   fun generate(input: Path, outputBase: Path) {

      val schema = Schema.Parser().parse(input.toFile())
      generate(schema)

      val spec = FileSpec.builder(schema.namespace, schema.name)
      types.forEach { spec.addType(it) }

      val outputPath = schema.namespace.split('.')
         .fold(outputBase) { acc, op -> acc.resolve(op) }
         .resolve(schema.name + ".kt")

      outputPath.parent.toFile().mkdirs()

      println("Writing to $outputBase")

      val contents = spec.build().toString()
      println("File contents $contents")
      Files.writeString(outputPath, contents)
   }

   private fun generate(field: Schema.Field): Pair<ParameterSpec, PropertySpec> {
      return when (field.schema().type) {
         Schema.Type.RECORD -> {
            generate(field.schema())
            val classref = ClassName(field.schema().namespace, field.schema().name)
            val param = ParameterSpec.builder(field.name(), classref).build()
            val prop = PropertySpec.builder(field.name(), classref)
               .initializer(field.name())
               .build()
            Pair(param, prop)
         }
         Schema.Type.ENUM -> TODO()
         Schema.Type.ARRAY -> TODO()
         Schema.Type.MAP -> TODO()
         Schema.Type.UNION -> TODO()
         Schema.Type.FIXED -> TODO()
         Schema.Type.STRING ->
            Pair(
               ParameterSpec.builder(field.name(), String::class).build(),
               PropertySpec.builder(field.name(), String::class).initializer(field.name()).build()
            )
         Schema.Type.BYTES -> TODO()
         Schema.Type.INT ->
            Pair(
               ParameterSpec.builder(field.name(), Int::class).build(),
               PropertySpec.builder(field.name(), Int::class).initializer(field.name()).build()
            )
         Schema.Type.LONG ->
            Pair(
               ParameterSpec.builder(field.name(), Long::class).build(),
               PropertySpec.builder(field.name(), Long::class).initializer(field.name()).build()
            )
         Schema.Type.FLOAT ->
            Pair(
               ParameterSpec.builder(field.name(), Float::class).build(),
               PropertySpec.builder(field.name(), Float::class).initializer(field.name()).build()
            )
         Schema.Type.DOUBLE ->
            Pair(
               ParameterSpec.builder(field.name(), Double::class).build(),
               PropertySpec.builder(field.name(), Double::class).initializer(field.name()).build()
            )
         Schema.Type.BOOLEAN ->
            Pair(
               ParameterSpec.builder(field.name(), Boolean::class).build(),
               PropertySpec.builder(field.name(), Boolean::class).initializer(field.name()).build()
            )
         Schema.Type.NULL -> TODO()
      }
   }

   private fun generate(schema: Schema): TypeSpec {

      val type = TypeSpec.classBuilder(schema.name)
         .addModifiers(KModifier.DATA)

      val constructor = FunSpec.constructorBuilder()
      schema.fields.map { field ->
         generate(field).apply {
            constructor.addParameter(first)
            type.addProperty(second)
         }
      }

      return type
         .primaryConstructor(constructor.build())
         .build()
         .apply {
            types.add(this)
         }
   }
}

