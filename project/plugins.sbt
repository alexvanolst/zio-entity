addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.5")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.2.24" )
addSbtPlugin("com.github.sbt"    % "sbt-pgp"      % "2.1.2")
addSbtPlugin("org.xerial.sbt"    % "sbt-sonatype" % "3.9.10")
addSbtPlugin("com.github.sbt" % "sbt-release"  % "1.1.0")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.33")

// project/plugins.sbt
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.7"