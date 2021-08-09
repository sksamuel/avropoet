object Ci {

   // this is the version used for building snapshots
   // .GITHUB_RUN_NUMBER-snapshot will be appended
   private const val snapshotBase = "1.0"
   const val org = "com.sksamuel.avropoet"

   private val githubRunNumber = System.getenv("GITHUB_RUN_NUMBER")

   private val snapshotVersion = when (githubRunNumber) {
      null -> "$snapshotBase.LOCAL"
      else -> "$snapshotBase.0.${githubRunNumber}-SNAPSHOT"
   }

   val gradleVersion = when (githubRunNumber) {
      null -> "$snapshotBase-LOCAL"
      else -> "$snapshotBase.${githubRunNumber}"
   }

   private val releaseVersion = System.getenv("RELEASE_VERSION")

   val isRelease = releaseVersion != null
   val publishVersion = releaseVersion ?: snapshotVersion
}
