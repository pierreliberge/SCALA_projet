package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame

trait Writer {
  def write(df: DataFrame, dstPath: String): Unit
}
