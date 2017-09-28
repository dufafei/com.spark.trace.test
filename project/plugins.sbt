logLevel := Level.Warn
resolvers += "sbt-plugin-releases" at "http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases"
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.8.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")