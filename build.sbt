

lazy val root = project
  .in(file("."))
  .settings(
    name := "dotty-simple",
    version := "0.1.0",
    resolvers += Resolver.mavenLocal,

    scalaVersion := "0.24.0-RC1", // "0.21.0-RC1", //"0.22.0-bin-20200114-193f7de-NIGHTLY", //dottyLatestNightlyBuild.get,

    scalacOptions ++= Seq(
      "-language:implicitConversions"
    ),

    libraryDependencies ++= Seq(
      ("com.lihaoyi" %% "pprint" % "0.5.6").withDottyCompat(scalaVersion.value),
      ("io.getquill" %% "quill-core-portable" % "3.4.11-SNAPSHOT" changing()).withDottyCompat(scalaVersion.value),
      ("io.getquill" %% "quill-sql-portable" % "3.4.11-SNAPSHOT" changing()).withDottyCompat(scalaVersion.value),
      "ch.epfl.lamp" % "dotty_0.24" % (scalaVersion.value),
      "org.scalatest" % "scalatest_0.24" % "3.1.2" % "test"
    )
  )
