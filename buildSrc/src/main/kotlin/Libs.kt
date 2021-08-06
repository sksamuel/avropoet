object Libs {

   const val kotlinVersion = "1.5.21"
   const val GradlePluginPublishVersion = "0.15.0"

   object Kotlin {
      const val reflect = "org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion"
      const val stdlib = "org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion"
   }

   object Kotest {
      private const val version = "4.6.1"
      const val assertions = "io.kotest:kotest-assertions-core-jvm:$version"
      const val junit5 = "io.kotest:kotest-runner-junit5-jvm:$version"
   }
}
