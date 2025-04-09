package fr.mosef.scala.template.reader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

trait Reader {
  def read(srcPath: String, format: String, options: Map[String, String]): DataFrame
}
