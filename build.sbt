import Dependencies._

val slickVersion = "3.2.3"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "slick-oracle-codegen",
    libraryDependencies ++= Seq(
        "com.typesafe.slick" %% "slick" % slickVersion
      , "org.slf4j" % "slf4j-nop" % "1.6.4"
      , "com.typesafe.slick" %% "slick-hikaricp" % slickVersion
      , "com.typesafe.slick" %% "slick-codegen" % slickVersion
      , "org.playframework.anorm" %% "anorm" % "2.6.2"
    ),
    libraryDependencies += scalaTest % Test
  )
