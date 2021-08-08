package com.sksamuel.avropoet

import com.squareup.kotlinpoet.CodeBlock
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.gradle.internal.impldep.org.bouncycastle.cert.ocsp.Req

fun encodeString(name: String): CodeBlock {
   return CodeBlock.builder().add("Utf8($name)").build()
}

fun encodeInt(name: String): CodeBlock {
   return CodeBlock.builder().add(name).build()
}

fun encodeBoolean(name: String): CodeBlock {
   return CodeBlock.builder().add(name).build()
}

fun encodeLong(name: String, schema: Schema): CodeBlock {
   return when (schema.logicalType) {
      is LogicalTypes.TimestampMillis -> CodeBlock.builder().add("$name.time").build()
      else -> CodeBlock.builder().add(name).build()
   }
}

fun encodeDouble(name: String): CodeBlock {
   return CodeBlock.builder().add(name).build()
}

fun encodeFloat(name: String): CodeBlock {
   return CodeBlock.builder().add(name).build()
}

fun encodeList(name: String, schema: Schema): CodeBlock {
   require(schema.type == Schema.Type.ARRAY) { "encodeList requires array schema, but was $schema" }
   return CodeBlock.builder()
      .addStatement("GenericData.Array(")
      .indent()
      .addStatement("schema.getField(%S).schema(),", name)
      .addStatement("$name.map { ${encode(schema.elementType, "it")} }")
      .unindent()
      .add(")")
      .build()
}

fun encodeEnum(name: String): CodeBlock {
   return CodeBlock.builder().add("GenericData.EnumSymbol(schema, $name.name)").build()
}

fun encode(schema: Schema, name: String): CodeBlock {
   return when (schema.type) {
      Schema.Type.RECORD -> encodeRecord(name)
      Schema.Type.ENUM -> encodeEnum(name)
      Schema.Type.ARRAY -> encodeList(name, schema)
      Schema.Type.MAP -> CodeBlock.builder().add("encodeMap(${name})", name).build()
      Schema.Type.UNION -> encodeUnion(name, schema)
      Schema.Type.FIXED -> TODO("b")
      Schema.Type.STRING -> encodeString(name)
      Schema.Type.BYTES -> TODO("b")
      Schema.Type.INT -> encodeInt(name)
      Schema.Type.LONG -> encodeLong(name, schema)
      Schema.Type.FLOAT -> encodeFloat(name)
      Schema.Type.DOUBLE -> encodeDouble(name)
      Schema.Type.BOOLEAN -> encodeBoolean(name)
      Schema.Type.NULL -> TODO("nullllls")
   }
}

fun encodeRecord(name: String): CodeBlock {
   return CodeBlock.builder().add("$name.encode(schema.elementType)").build()
}

fun encodeUnion(name: String, schema: Schema): CodeBlock {
   require(schema.isNullableUnion())
   return CodeBlock.builder().add("$name?.let { ${encode(schema.types[1], name)} }").build()
}
