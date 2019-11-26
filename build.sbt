import sbt.Resolver
// Rename this as you see fit
name := "simplesearch"

version := "0.2.0"

scalaVersion := "2.11.12"

organization := "ken.thesis"

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials")

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

// SBT Spark needs forking when run in SBT CLI
fork in run := true
outputStrategy in run := Some(StdoutOutput)
connectInput in run := true

// We need to bump up the memory for some of the examples working with the landsat image.
javaOptions += "-Xmx4G"


resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.bintrayRepo("bkirwi", "maven"), // Required for `decline` dependency
  Resolver.bintrayRepo("azavea", "maven"),
  Resolver.bintrayRepo("azavea", "geotrellis"),
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  "OSGeo Repository" at "http://download.osgeo.org/webdav/geotools/",
  "Boundless Repository" at "http://repo.boundlessgeo.com/main/",
//  "GeoMajas" at "http://maven.geomajas.org/",
  "efarmer" at "http://dev.efarmer.mobi:8889/repository/internal/",
    "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
  "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots"
//  Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
)

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "2.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-proj4" % "2.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-geotools" % "2.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-shapefile" % "2.1.0",

  "org.apache.spark" %% "spark-core" % "2.3.1",
//  "org.apache.spark" %% "spark-core" % "2.2.0",
//  "org.apache.hadoop" %% "hadoop-client"         % "2.7.7",

  "com.lihaoyi" %% "pprint" % "0.4.3",
  "org.scalatest"         %%  "scalatest"       % "2.2.0" % Test

)

// When creating fat jar, remote some files with
// bad signatures and resolve conflicts by taking the first
// versions of shared packaged types.
assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

initialCommands in console := """
 |import geotrellis.raster._
 |import geotrellis.vector._
 |import geotrellis.proj4._
 |import geotrellis.spark._
 |import geotrellis.spark.io._
 |import geotrellis.spark.io.hadoop._
 |import geotrellis.spark.tiling._
 |import geotrellis.spark.util._
 """.stripMargin
