name := "dielectric"

version := "0.0.1"

scalaVersion := "2.10.5"

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

resolvers ++= Seq(
  "bintray/non" at "http://dl.bintray.com/non/maven",
  Resolver.sonatypeRepo("releases")
)

val scalazVersion = "7.1.0"

val sparkVersion = "1.3.0"

libraryDependencies ++= Seq(
  compilerPlugin("org.scalamacros"  % "paradise_2.10.5" % "2.0.1"),
  compilerPlugin("org.spire-math"   %% "kind-projector" % "0.5.2"),

  "org.scalaz"        %% "scalaz-concurrent"  % scalazVersion,
  "org.scalaz"        %% "scalaz-core"        % scalazVersion,
  "org.scalaz"        %% "scalaz-effect"      % scalazVersion,
  "com.chuusai"       %  "shapeless_2.10.5"   % "2.2.0-RC1",
  "org.apache.spark"  %% "spark-core"         % sparkVersion,
  "org.apache.spark"  %% "spark-sql"          % sparkVersion,
  "org.apache.spark"  %% "spark-streaming"    % sparkVersion,
  "org.spire-math"    %% "spire"              % "0.9.0"
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
