package com.sksamuel.avropoet

import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.TypeSpec
import org.apache.avro.Schema
import java.nio.file.Path

class AvroPoet {

  fun generate(file: Path) {
    val schema = Schema.Parser().parse(file.toFile())

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

    spec.writeTo(file.resolveSibling(schema.name + ".kt"))

  }
}
