package fr.mosef.scala.template.reader.impl
import fr.mosef.scala.template.reader.Reader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  override def read(srcPath: String, format: String, options: Map[String, String]): DataFrame = {
    format match {
      case "csv" =>
        sparkSession.read
          .option("header", options.getOrElse("header", "true"))
          .option("sep", options.getOrElse("sep", ","))
          .option("inferSchema", options.getOrElse("inferSchema", "true"))
          .option("multiLine", options.getOrElse("multiLine", "true"))  // Gère les valeurs multi-lignes
          .option("quote", options.getOrElse("quote", "\""))            // Gère les guillemets
          .option("escape", options.getOrElse("escape", "\""))          // Échappe les guillemets
          .option("encoding", options.getOrElse("encoding", "UTF-8"))   // Force l'encodage UTF-8
          .csv(srcPath)
      case "parquet" =>
        sparkSession.read.parquet(srcPath)
      case "json" =>
        sparkSession.read
          .option("multiline", "true")
          .json(srcPath)
      case _ =>
        throw new IllegalArgumentException(s"Format de fichier non supporté : $format")


    }
  }
}
