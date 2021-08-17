package com.sksamuel.avropoet

import org.gradle.api.Plugin
import org.gradle.api.Project
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

         val entries = entries(inputBase)
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
