name := "dielectric"

version := "0.0.1"

scalaVersion := "2.10.5"

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

val scalazVersion = "7.1.0"

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-concurrent"  % scalazVersion,
  "org.scalaz"        %% "scalaz-core"        % scalazVersion,
  "org.scalaz"        %% "scalaz-effect"      % scalazVersion,
  "org.apache.spark"  %% "spark-core"         % "1.3.0"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)
