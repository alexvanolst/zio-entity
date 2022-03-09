addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.3.0" )
addSbtPlugin("com.github.sbt"    % "sbt-pgp"      % "2.1.2")
addSbtPlugin("org.xerial.sbt"    % "sbt-sonatype" % "3.9.11")
addSbtPlugin("com.github.sbt" % "sbt-release"  % "1.1.0")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.33")

// project/plugins.sbt
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.8"