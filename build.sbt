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
    resolvers += "OAM 11g" at "https://maven.oracle.com",
    credentials += Credentials("OAM 11g", "login.oracle.com", "email", "pass"),
    libraryDependencies ++= Seq(
        "com.typesafe.slick" %% "slick" % slickVersion
      , "org.slf4j" % "slf4j-nop" % "1.6.4"
      , "com.typesafe.slick" %% "slick-hikaricp" % slickVersion
      , "com.typesafe.slick" %% "slick-codegen" % slickVersion
      , "org.playframework.anorm" %% "anorm" % "2.6.2"
      , "com.oracle.jdbc" % "ojdbc8" % "18.3.0.0" exclude("com.oracle.jdbc", "ucp")
    ),
    libraryDependencies += scalaTest % Test
  )


/*
mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc14 \
     -Dversion=10.2.0.3.0 -Dpackaging=jar -Dfile=ojdbc.jar -DgeneratePom=true
*/