name := "com.github.gzilt.text.classifier"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.12.1" % "provided",
  "org.apache.spark"        %% "spark-core"               % "2.1.1" % "provided",
  "org.apache.spark"        %% "spark-mllib"              % "2.1.1" % "provided",
  "org.apache.lucene"        % "lucene-core"              % "6.5.1",
  "org.apache.lucene"        % "lucene-analyzers-common"  % "6.5.1",
  "org.xerial.snappy"        % "snappy-java"              % "1.1.1.7",
  "com.esotericsoftware"     % "kryo"                     % "4.0.1"
)
