package fr.mosef.scala.template.writer.impl

import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.DataFrame

class PartitionerImpl extends Writer {
  override def write(df: DataFrame, dstPath: String): Unit = {
    df.printSchema()
    df.write
      .mode("overwrite")
      .parquet(dstPath)

    println(s"✅ Données écrites en Parquet avec partitionnement par `nature_juridique_du_rappel` dans : $dstPath")
  }
}
