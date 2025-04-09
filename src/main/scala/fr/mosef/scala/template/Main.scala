package fr.mosef.scala.template
import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.writer.Writer
import fr.mosef.scala.template.writer.impl.PartitionerImpl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

object Main extends App with Job {

  val cliArgs = args

  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case _: ArrayIndexOutOfBoundsException => "local[1]"
  }

  val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case _: ArrayIndexOutOfBoundsException =>
      println("❌ Aucun fichier source spécifié.")
      sys.exit(1)
  }

  val DST_PATH: String = try {
    cliArgs(2)
  } catch {
    case _: ArrayIndexOutOfBoundsException => "./default/output-writer"
  }

  val FORMAT: String = try {
    cliArgs(3)
  } catch {
    case _: ArrayIndexOutOfBoundsException => "csv" // Par défaut, CSV
  }

  // Configuration Spark
  val conf = new SparkConf()
  conf.set("spark.driver.memory", "64M")
  conf.set("spark.testing.memory", "471859200")

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Spark Job - CSV to Parquet")
    .getOrCreate()

  // Configuration Hadoop pour éviter les erreurs locales
  sparkSession
    .sparkContext
    .hadoopConfiguration
    .setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])

  // Affichage des paramètres
  println("🚀 Lancement du job avec les paramètres suivants :")
  println(s"📥 Chemin source : $SRC_PATH")
  println(s"📤 Chemin destination : $DST_PATH")
  println(s"📂 Format d'entrée : $FORMAT")

  // Initialisation des composants du Job
  override val reader: Reader = new ReaderImpl(sparkSession)
  override val processor: Processor = new ProcessorImpl()
  override val writer: Writer = new PartitionerImpl()
  override val srcPath: String = SRC_PATH
  override val dstPath: String = DST_PATH
  override val format: String = FORMAT
  override val options: Map[String, String] = Map(
    "header" -> "true",
    "sep" -> ",",
    "inferSchema" -> "true",
    "multiLine" -> "true",  // Gère les valeurs multi-lignes
    "quote" -> "\"",        // Gère les guillemets
    "escape" -> "\"",       // Échappe les guillemets
    "encoding" -> "UTF-8"   // Force l'encodage UTF-8
  )
  // Exécution du pipeline ETL
  val inputDF: DataFrame = reader.read(srcPath, format, options)
  val processedDF: DataFrame = processor.process(inputDF)
  writer.write(processedDF, dstPath)

  // Fermeture de Spark
  sparkSession.stop()
  println("✅ Traitement terminé avec succès !")
}
