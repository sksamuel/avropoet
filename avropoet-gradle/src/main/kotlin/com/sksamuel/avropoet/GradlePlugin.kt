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

         val outputBase = project.projectDir.toPath().resolve("src/main/kotlin")
         val poet = AvroPoet(outputBase)

         val src = project.projectDir.toPath().resolve("src/main/avro")
         entries(src)
            .sortedBy { if (it.toString().contains("/shared/")) -1 else 1 }
            .filter { it.isRegularFile() }
            .filter { it.name.endsWith(".json") || it.name.endsWith(".avro") || it.name.endsWith(".avdl") }
            .forEach {
               println("Processing $it")
               poet.reset()
               poet.generate(it)
            }
      }
      task.description = "Generate kotlin data classes from avro definitions"
      task.group = "avropoet"
   }
}
