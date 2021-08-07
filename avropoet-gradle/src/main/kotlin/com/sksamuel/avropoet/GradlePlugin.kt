package com.sksamuel.avropoet

import org.gradle.api.Plugin
import org.gradle.api.Project
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name

@ExperimentalPathApi
class GradlePlugin : Plugin<Project> {
   override fun apply(project: Project) {
      val task = project.task("generateSources") {
         val src = project.projectDir.toPath().resolve("src/main/avro")
         val outputBase = project.projectDir.toPath().resolve("src/main/kotlin")
         src.listDirectoryEntries()
            .filter { it.name.endsWith(".json") || it.name.endsWith(".avro") || it.name.endsWith(".avdl") }
            .forEach {
               println("Processing $it")
               AvroPoet().recprd(it, outputBase)
            }
      }
      task.description = "Generate kotlin data classes from avro definitions"
      task.group = "avropoet"
   }
}
