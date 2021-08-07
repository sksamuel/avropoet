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
import com.squareup.kotlinpoet.TypeVariableName
import com.squareup.kotlinpoet.asClassName
import com.squareup.kotlinpoet.asTypeName
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.nio.file.Files
import java.nio.file.Path
import kotlin.reflect.KClass

class AvroPoet {

   private val types = mutableListOf<TypeSpec>()
   private val encoders = mutableListOf<FunSpec>()

   private fun encodePrimitiveFn(type: KClass<*>) =
      FunSpec.builder("encode${type.simpleName}")
         .addModifiers(KModifier.PRIVATE)
         .addParameter("value", type)
         .returns(type)
         .addCode("return value")
         .build()

   private val encodeList =
      FunSpec.builder("encodeList")
         .addModifiers(KModifier.PRIVATE)
         .addTypeVariable(TypeVariableName("T", Any::class))
         .addParameter("value", List::class.asClassName().parameterizedBy(TypeVariableName("T")))
         .addParameter("schema", Schema::class.asClassName())
         .returns(GenericData.Array::class.asTypeName().parameterizedBy(TypeVariableName("T")))
         .addCode("return GenericData.Array(schema, value)")
         .build()

   private val encodeMap =
      FunSpec.builder("encodeMap")
         .addModifiers(KModifier.PRIVATE)
         .addTypeVariable(TypeVariableName("V", Any::class))
         .addParameter(
            "value",
            Map::class.asClassName().parameterizedBy(listOf(String::class.asTypeName(), TypeVariableName("V")))
         )
         .returns(Map::class.asClassName().parameterizedBy(String::class.asTypeName(), TypeVariableName("V")))
         .addCode("return value as Map<String, V>")
         .build()

   private val decodeList =
      FunSpec.builder("decodeList")
         .addModifiers(KModifier.PRIVATE)
         .addTypeVariable(TypeVariableName("T", Any::class))
         .addParameter("name", String::class)
         .addParameter("record", GenericRecord::class)
         .returns(List::class.asClassName().parameterizedBy(TypeVariableName("T")))
         .addCode("return record.get(name) as List<T>")
         .build()

   private val decodeMap =
      FunSpec.builder("decodeMap")
         .addModifiers(KModifier.PRIVATE)
         .addTypeVariable(TypeVariableName("V", Any::class))
         .addParameter("name", String::class)
         .addParameter("record", GenericRecord::class)
         .returns(Map::class.asClassName().parameterizedBy(String::class.asTypeName(), TypeVariableName("V")))
         .addCode("return record.get(name) as Map<String, V>")
         .build()

   private fun decodePrimitiveFn(type: KClass<*>) =
      FunSpec.builder("decode${type.simpleName}")
         .addModifiers(KModifier.PRIVATE)
         .addParameter("name", String::class)
         .addParameter("record", GenericRecord::class)
         .returns(type)
         .addCode("return record.get(name) as ${type.simpleName}")
         .build()


   fun generate(input: Path, outputBase: Path) {

      val schema = Schema.Parser().parse(input.toFile())
      record(schema)

      val spec = FileSpec.builder(schema.namespace, schema.name)
      spec.addImport(GenericData::class.java.`package`.name, "GenericData")
      spec.addFunction(encodePrimitiveFn(String::class))
      spec.addFunction(encodePrimitiveFn(Long::class))
      spec.addFunction(encodePrimitiveFn(Double::class))
      spec.addFunction(encodePrimitiveFn(Int::class))
      spec.addFunction(encodePrimitiveFn(Float::class))
      spec.addFunction(encodePrimitiveFn(Boolean::class))
      spec.addFunction(encodeList)
      spec.addFunction(encodeMap)

      spec.addFunction(decodePrimitiveFn(String::class))
      spec.addFunction(decodePrimitiveFn(Long::class))
      spec.addFunction(decodePrimitiveFn(Double::class))
      spec.addFunction(decodePrimitiveFn(Int::class))
      spec.addFunction(decodePrimitiveFn(Float::class))
      spec.addFunction(decodePrimitiveFn(Boolean::class))

      spec.addFunction(decodeList)
      spec.addFunction(decodeMap)
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

   private fun encodeField(field: Schema.Field): CodeBlock {
      return when (field.schema().type) {
         Schema.Type.RECORD -> CodeBlock.builder().add("encodeInt(${field.name()})").build()
         Schema.Type.ENUM -> TODO()
         Schema.Type.ARRAY -> CodeBlock.builder()
            .add("encodeList(${field.name()}, schema.getField(%S).schema())", field.name()).build()
         Schema.Type.MAP -> CodeBlock.builder().add("encodeMap(${field.name()})", field.name()).build()
         Schema.Type.UNION -> TODO()
         Schema.Type.FIXED -> TODO()
         Schema.Type.STRING -> CodeBlock.builder().add("encodeString(${field.name()})", field.name()).build()
         Schema.Type.BYTES -> TODO()
         Schema.Type.INT -> CodeBlock.builder().add("encodeInt(${field.name()})", field.name()).build()
         Schema.Type.LONG -> CodeBlock.builder().add("encodeLong(${field.name()})", field.name()).build()
         Schema.Type.FLOAT -> CodeBlock.builder().add("encodeFloat(${field.name()})", field.name()).build()
         Schema.Type.DOUBLE -> CodeBlock.builder().add("encodeDouble(${field.name()})", field.name()).build()
         Schema.Type.BOOLEAN -> CodeBlock.builder().add("encodeBoolean(${field.name()})", field.name()).build()
         Schema.Type.NULL -> TODO()
      }
   }

   private fun decodeField(field: Schema.Field): CodeBlock {
      return when (field.schema().type) {
         Schema.Type.RECORD -> CodeBlock.builder().addStatement("decodeInt(%S, record),", field.name()).build()
         Schema.Type.ENUM -> TODO()
         Schema.Type.ARRAY -> CodeBlock.builder().addStatement("decodeList(%S, record),", field.name()).build()
         Schema.Type.MAP -> CodeBlock.builder().addStatement("decodeMap(%S, record),", field.name()).build()
         Schema.Type.UNION -> TODO()
         Schema.Type.FIXED -> TODO()
         Schema.Type.STRING -> CodeBlock.builder().addStatement("decodeString(%S, record),", field.name()).build()
         Schema.Type.BYTES -> TODO()
         Schema.Type.INT -> CodeBlock.builder().addStatement("decodeInt(%S, record),", field.name()).build()
         Schema.Type.LONG -> CodeBlock.builder().addStatement("decodeLong(%S, record),", field.name()).build()
         Schema.Type.FLOAT -> CodeBlock.builder().addStatement("decodeFloat(%S, record),", field.name()).build()
         Schema.Type.DOUBLE -> CodeBlock.builder().addStatement("decodeDouble(%S, record),", field.name()).build()
         Schema.Type.BOOLEAN -> CodeBlock.builder().addStatement("decodeBoolean(%S, record),", field.name()).build()
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

      decoder.addCode("return ${schema.name}(\n")
      schema.fields.forEach {
         decoder.addCode(decodeField(it))
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
         encoder.addStatement("record.put(%S, ${encodeField(it)})", it.name())
      }
      encoder.addStatement("return record")
         .build()
         .apply { encoders.add(this) }

      return ref
   }
}

