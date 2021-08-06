package com.sksamuel.avropoet

import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.TypeSpec
import org.apache.avro.Schema
import java.nio.file.Path

object AvroPoet {

   fun generate(input: Path, outputBase: Path) {
      val schema = Schema.Parser().parse(input.toFile())

      val spec: FileSpec = FileSpec.builder(schema.namespace, schema.name + ".kt")
         .addType(
            TypeSpec.classBuilder(schema.name)
               .primaryConstructor(
                  FunSpec.constructorBuilder()
                     .addParameter("name", String::class)
                     .build()
               )
               .build()
         ).build()

      val outputPath = schema.namespace.split('.')
         .fold(outputBase) { acc, op -> acc.resolve(op) }
         .resolve(schema.name + ".kt")

      spec.writeTo(outputPath)

   }
}
