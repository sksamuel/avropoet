@file:Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")

package com.sksamuel.avropoet

import com.squareup.kotlinpoet.CodeBlock
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema

fun decode(schema: Schema, name: String): CodeBlock {
   return when (schema.type) {
      Schema.Type.RECORD -> decodeRecord(name, schema)
      Schema.Type.ENUM -> decodeEnum(name, schema)
      Schema.Type.ARRAY -> decodeList(name, schema)
      Schema.Type.MAP -> decodeMap(name, schema)
      Schema.Type.UNION -> decodeUnion(name, schema)
      Schema.Type.FIXED -> TODO("f")
      Schema.Type.STRING -> decodeString(name)
      Schema.Type.BYTES -> decodeBytes(name)
      Schema.Type.INT -> decodeInt(name)
      Schema.Type.LONG -> decodeLong(name, schema)
      Schema.Type.FLOAT -> decodeFloat(name)
      Schema.Type.DOUBLE -> decodeDouble(name)
      Schema.Type.BOOLEAN -> decodeBoolean(name)
      Schema.Type.NULL -> TODO("n")
   }
}

fun decodeBoolean(name: String): CodeBlock = CodeBlock.builder().add("$name as Boolean").build()
fun decodeFloat(name: String): CodeBlock = CodeBlock.builder().add("$name as Float").build()
fun decodeDouble(name: String): CodeBlock = CodeBlock.builder().add("$name as Double").build()
fun decodeInt(name: String): CodeBlock = CodeBlock.builder().add("$name as Int").build()

fun decodeBytes(name: String): CodeBlock {
   return CodeBlock.builder().addStatement("when ($name) {")
      .indent()
      .addStatement("is ByteArray -> $name")
      .addStatement("is ByteBuffer -> $name.array()")
      .addStatement("else -> error(\"Unknown bytes type $$name\")")
      .unindent()
      .add("}")
      .build()
}

fun decodeRecord(name: String, schema: Schema): CodeBlock {
   return CodeBlock.builder().add("${schema.name}.decode($name as GenericRecord)").build()
}

fun decodeMap(name: String, schema: Schema): CodeBlock {
   return CodeBlock.builder().addStatement("when ($name) {")
      .indent()
      .addStatement("is Map<*,*> -> $name.map { it.key.toString() to ${decode(schema.valueType, "it.value")} }.toMap()")
      .addStatement("else -> error(\"Unknown type for map schema $$name\")")
      .unindent()
      .add("}")
      .build()
}

fun decodeList(name: String, schema: Schema): CodeBlock {
   return CodeBlock.builder().addStatement("when ($name) {")
      .indent()
      .addStatement("is List<*> -> $name.map { ${decode(schema.elementType, "it")} }")
      .addStatement("else -> error(\"Unknown list type $$name\")")
      .unindent()
      .add("}")
      .build()
}

fun decodeString(name: String): CodeBlock {
   return CodeBlock.builder().addStatement("when ($name) {")
      .indent()
      .addStatement("is String -> $name")
      .addStatement("is Utf8 -> $name.toString()")
      .addStatement("else -> error(\"Unknown string type $$name\")")
      .unindent()
      .add("}")
      .build()
}

fun decodeEnum(name: String, schema: Schema): CodeBlock {
   val enumClass = schema.name
   return CodeBlock.builder()
      .add("$enumClass.valueOf($name.toString())", name)
      .build()
}

fun decodeUnion(name: String, schema: Schema): CodeBlock {
   require(schema.isNullableUnion())
   return CodeBlock.builder()
      .add("$name?.let { ${decode(schema.types[1], "it")} }", name)
      .build()
}

fun decodeLong(name: String, schema: Schema): CodeBlock {
   return when (schema.logicalType) {
      is LogicalTypes.TimestampMillis -> CodeBlock.builder().add("Timestamp($name as Long)").build()
      else -> CodeBlock.builder().add("$name as Long").build()
   }
}
