package com.sksamuel.avropoet

import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeSpec
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.gradle.api.Plugin
import org.gradle.api.Project
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.isRegularFile
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name

@ExperimentalPathApi
class GradlePlugin : Plugin<Project> {

   private fun entries(src: Path): List<Path> {
      return src.listDirectoryEntries().flatMap {
         if (it.isRegularFile()) listOf(it) else entries(it)
      }
   }

   override fun apply(project: Project) {
      val task = project.task("generateSources") {

         val inputBase = project.projectDir.toPath().resolve("src/main/avro")
         val outputBase = project.projectDir.toPath().resolve("src/main/kotlin")

         val interfaces = FileSpec.builder("com.sksamuel.avropoet", "interfaces.kt")
            .addType(
               TypeSpec.interfaceBuilder("HasEncoder").addFunction(
                  FunSpec.builder("encode").returns(GenericRecord::class).addModifiers(KModifier.ABSTRACT).build()
               ).build()
            )
            .addType(
               TypeSpec.interfaceBuilder("HasSchema").addProperty(
                  PropertySpec.builder("schema", Schema::class, KModifier.OVERRIDE).build()
               ).build()
            )
            .build()

         val outputPath = "com.sksamuel.avropoet".split('.')
            .fold(outputBase) { acc, op -> acc.resolve(op) }
            .resolve("interfaces.kt")

         outputPath.parent.toFile().mkdirs()
         println("Writing to $outputBase")

         Files.write(outputPath, interfaces.toString().encodeToByteArray())

         val entries = entries(inputBase)
            .sortedBy { it.toString().toLowerCase() }
            .filter { it.isRegularFile() }
            .filter { it.name.endsWith(".json") || it.name.endsWith(".avro") || it.name.endsWith(".avdl") }

         val (shared, records) = entries.partition { it.toString().contains("/shared/") }

         shared.forEach {
            println("Processing $it")
            AvroPoet(inputBase, outputBase, emptyList()).generate(it)
         }

         records.forEach {
            println("Processing $it")
            AvroPoet(inputBase, outputBase, shared).generate(it)
         }
      }
      task.description = "Generate kotlin data classes from avro definitions"
      task.group = "avropoet"
   }
}
