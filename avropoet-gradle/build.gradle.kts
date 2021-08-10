plugins {
   kotlin("jvm")
   id("java-library")
   id("maven-publish")
   id("java-gradle-plugin")
   id("com.gradle.plugin-publish").version(Libs.GradlePluginPublishVersion)
}

version = Ci.gradleVersion

dependencies {
   implementation("org.apache.avro:avro:1.10.2")
   implementation("com.squareup:kotlinpoet:1.9.0")
   compileOnly("org.jetbrains.kotlin:kotlin-gradle-plugin:1.5.21")
}

tasks {
   pluginBundle {
      website = "https://github.com/sksamuel/avropoet"
      vcsUrl = "https://github.com/sksamuel/avropoet"
      tags = listOf("avro", "kotlin")
   }
   gradlePlugin {
      plugins {
         create("avropoetPlugin") {
            id = "com.sksamuel.avropoet"
            implementationClass = "com.sksamuel.avropoet.GradlePlugin"
            displayName = "Avropoet Gradle Plugin"
            description = "Generates sources from avro definitions"
         }
      }
   }
}
