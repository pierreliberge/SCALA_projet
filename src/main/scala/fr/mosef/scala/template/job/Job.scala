package fr.mosef.scala.template.job
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.writer.Writer
import fr.mosef.scala.template.writer.impl.PartitionerImpl
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

trait Job {
  val reader: Reader
  val processor: Processor
  val writer: Writer
  val srcPath: String
  val dstPath: String
  val format: String
  val options: Map[String, String] = Map(
    "header" -> "true",
    "sep" -> ",",
    "inferSchema" -> "true",
    "multiLine" -> "true",  // Gère les valeurs multi-lignes
    "quote" -> "\"",        // Gère les guillemets
    "escape" -> "\"",       // Échappe les guillemets
    "encoding" -> "UTF-8"   // Force l'encodage UTF-8
  )
  def run()(implicit spark: SparkSession): Unit = {
    try {
      println(s"📥 Lecture du fichier CSV depuis : $srcPath")
      val inputDF: DataFrame = reader.read(srcPath, format, options)

      println("🔄 Transformation des données...")
      val processedDF: DataFrame = processor.process(inputDF)

      println(s"💾 Écriture des données en Parquet avec partitionnement vers : $dstPath")
      writer.write(processedDF, dstPath)

      println("✅ Job terminé avec succès !")
    } catch {
      case e: Exception =>
        println(s"❌ Erreur dans le job : ${e.getMessage}")
        throw e
    }
  }
}
