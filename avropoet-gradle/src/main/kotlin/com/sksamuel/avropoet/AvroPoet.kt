package com.sksamuel.avropoet

import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
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
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Timestamp

class AvroPoet {

   private val types = mutableListOf<TypeSpec>()
   private val encoders = mutableListOf<FunSpec>()

   fun generate(input: Path, outputBase: Path) {

      val schema = Schema.Parser().parse(input.toFile())
      record(schema)

      val spec = FileSpec.builder(schema.namespace, schema.name)

      spec.addImport(GenericData::class.java.`package`.name, "GenericData")
      spec.addImport(Utf8::class.java.`package`.name, "Utf8")
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
         Schema.Type.ENUM -> enum(schema)
         Schema.Type.ARRAY -> ClassName("kotlin.collections", "List").parameterizedBy(ref(schema.elementType))
         Schema.Type.MAP -> ClassName("kotlin.collections", "Map").parameterizedBy(
            String::class.asClassName(),
            ref(schema.valueType)
         )
         Schema.Type.UNION -> union(schema)
         Schema.Type.FIXED -> TODO("ref fixed")
         Schema.Type.STRING -> String::class.asTypeName()
         Schema.Type.BYTES -> ByteArray::class.asTypeName()
         Schema.Type.INT -> Int::class.asTypeName()
         Schema.Type.LONG -> schemaForLong(schema)
         Schema.Type.FLOAT -> Float::class.asTypeName()
         Schema.Type.DOUBLE -> Double::class.asTypeName()
         Schema.Type.BOOLEAN -> Boolean::class.asTypeName()
         Schema.Type.NULL -> TODO("nnnn")
         null -> error("Invalid code path")
      }
   }

   private fun schemaForLong(schema: Schema): TypeName {
      return when (schema.logicalType) {
         is LogicalTypes.TimestampMillis -> Timestamp::class.asTypeName()
         else -> Long::class.asTypeName()
      }
   }

   private fun encodeString(name: String): CodeBlock {
      return CodeBlock.builder().add("Utf8($name)").build()
   }

   private fun encodeInt(name: String): CodeBlock {
      return CodeBlock.builder().add(name).build()
   }

   private fun encodeBoolean(name: String): CodeBlock {
      return CodeBlock.builder().add(name).build()
   }

   private fun encodeLong(name: String, schema: Schema): CodeBlock {
      return when (schema.logicalType) {
         is LogicalTypes.TimestampMillis -> CodeBlock.builder().add("$name.time").build()
         else -> CodeBlock.builder().add(name).build()
      }
   }

   private fun encodeDouble(name: String): CodeBlock {
      return CodeBlock.builder().add(name).build()
   }

   private fun encodeFloat(name: String): CodeBlock {
      return CodeBlock.builder().add(name).build()
   }

   private fun encodeList(name: String, schema: Schema): CodeBlock {
      return CodeBlock.builder().add("GenericData.Array(schema.getField(%S).schema().elementType, $name)", name).build()
   }

   private fun encodeEnum(name: String, schema: Schema): CodeBlock {
      return CodeBlock.builder().add("GenericData.EnumSymbol(schema, $name.name)").build()
   }

   private fun decodeString(name: String, schema: Schema): CodeBlock {
      return CodeBlock.builder().addStatement("when($name) {", name)
         .addStatement("is String -> $name")
         .addStatement("is Utf8 -> $name.toString()")
         .addStatement("else -> error(\"Unknown string type $$name\")")
         .add("}")
         .build()
   }

   private fun encode(schema: Schema, name: String): CodeBlock {
      return when (schema.type) {
         Schema.Type.RECORD -> CodeBlock.builder().add("encodeInt(${name})").build()
         Schema.Type.ENUM -> encodeEnum(name, schema)
         Schema.Type.ARRAY -> encodeList(name, schema.elementType)
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

   private fun encodeUnion(name: String, schema: Schema): CodeBlock {
      require(schema.isNullableUnion())
      return CodeBlock.builder().add("$name?.let { ${encode(schema.types[1], name)} }").build()
   }

   private fun Schema.isNullableUnion(): Boolean {
      return isUnion && types.size == 2 && types[0].type == Schema.Type.NULL && types[1].type != Schema.Type.NULL
   }

   private fun decode(schema: Schema, name: String): CodeBlock {
      return when (schema.type) {
         Schema.Type.RECORD -> CodeBlock.builder().addStatement("decodeInt(%S, record)", name).build()
         Schema.Type.ENUM -> decodeEnum(name, schema)
         Schema.Type.ARRAY -> CodeBlock.builder().addStatement("decodeList(%S, record)", name).build()
         Schema.Type.MAP -> CodeBlock.builder().addStatement("decodeMap(%S, record)", name).build()
         Schema.Type.UNION -> decodeUnion(name, schema)
         Schema.Type.FIXED -> TODO("f")
         Schema.Type.STRING -> decodeString(name, schema)
         Schema.Type.BYTES -> TODO()
         Schema.Type.INT -> CodeBlock.builder().addStatement("decodeInt(%S, record)", name).build()
         Schema.Type.LONG -> decodeLong(name, schema)
         Schema.Type.FLOAT -> CodeBlock.builder().addStatement("decodeFloat(%S, record)", name).build()
         Schema.Type.DOUBLE -> CodeBlock.builder().addStatement("decodeDouble(%S, record)", name).build()
         Schema.Type.BOOLEAN -> CodeBlock.builder().addStatement("decodeBoolean(%S, record)", name).build()
         Schema.Type.NULL -> TODO("n")
      }
   }

   private fun decodeEnum(name: String, schema: Schema): CodeBlock {
      val enumClass = schema.name
      return CodeBlock.builder()
         .add("$enumClass.valueOf(record.get(%S).toString())", name)
         .build()
   }

   private fun decodeUnion(name: String, schema: Schema): CodeBlock {
      require(schema.isNullableUnion())
      return CodeBlock.builder()
         .add("record.get(%S)?.let { ${decode(schema.types[1], name)} }", name)
         .build()
   }

   private fun decodeLong(name: String, schema: Schema): CodeBlock {
      return when (schema.logicalType) {
         is LogicalTypes.TimestampMillis -> CodeBlock.builder().add("java.sql.Timestamp($name as Long)").build()
         else -> CodeBlock.builder().add("$name as Long").build()
      }
   }

   private fun enum(schema: Schema): ClassName {
      val builder = TypeSpec.enumBuilder(schema.name)
      schema.enumSymbols.forEach { builder.addEnumConstant(it) }
      builder.build().apply { types.add(this) }
      return ClassName(schema.namespace, schema.name)
   }

   private fun union(schema: Schema): TypeName {
      require(schema.isUnion)
      require(schema.types.size == 2)

      require(schema.types.first().type == Schema.Type.NULL) { "Only unions of null,other are supported" }
      require(schema.types[1].type != Schema.Type.NULL) { "Only unions of null,other are supported" }

      return ref(schema.types[1]).copy(nullable = true)
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

      schema.fields.forEach {
         decoder.addStatement("val ${it.name()} = record.get(%S)", it.name())
      }

      decoder.addCode("return ${schema.name}(\n")
      schema.fields.forEach {
         decoder.addCode(decode(it.schema(), it.name()).toBuilder().add(",\n").build())
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
         encoder.addStatement("record.put(%S, ${encode(it.schema(), it.name())})", it.name())
      }
      encoder.addStatement("return record")
         .build()
         .apply { encoders.add(this) }

      return ref
   }
}

