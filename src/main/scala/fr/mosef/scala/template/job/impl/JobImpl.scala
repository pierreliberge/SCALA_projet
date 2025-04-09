package fr.mosef.scala.template.job.impl

import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import java.io.File
import fr.mosef.scala.template.writer.Writer
import fr.mosef.scala.template.writer.impl.PartitionerImpl
import fr.mosef.scala.template.job.Job
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class JobImpl(implicit spark: SparkSession) extends Job {


  // Initialisation de Reader
  val reader: Reader = new ReaderImpl(spark)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new PartitionerImpl()

  // Spécification des chemins et du format
  val srcPath: String = "src/main/resources/rappelconso0.csv"
  val dstPath: String = "tmp/output"
  val format: String = "csv"  // Ou "parquet"

  override val options: Map[String, String] = Map(
    "header" -> "true",
    "sep" -> ",",
    "inferSchema" -> "true",
    "multiLine" -> "true",  // Gère les valeurs multi-lignes
    "quote" -> "\"",        // Gère les guillemets
    "escape" -> "\"",       // Échappe les guillemets
    "encoding" -> "UTF-8"   // Force l'encodage UTF-8
  )
  // Appel à la méthode `read` avec les bons paramètres
  val inputDF: DataFrame = reader.read(srcPath, format, options)

  // Traitement des données
  val processedDF: DataFrame = processor.process(inputDF)

  if (!new File(srcPath).exists()) {
    throw new IllegalArgumentException(s"Le fichier source n'existe pas : $srcPath")
  }
  // Écriture des données traitées
  writer.write(processedDF, dstPath)
}
