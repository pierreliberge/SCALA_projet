package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ProcessorImpl extends Processor {
  override def process(inputDF: DataFrame): DataFrame = {
    inputDF
      .dropDuplicates() // Suppression des doublons parfait
      .withColumn("processed_at", current_timestamp()) // Ajout d'un timestamp
      // Convertir 'reference_fiche' au format date (2021-03-01)
     .withColumn("date_de_fin_de_la_procedure_de_rappel",
        regexp_replace(col("date_de_fin_de_la_procedure_de_rappel"),
          "dimanche|lundi|mardi|mercredi|jeudi|vendredi|samedi", ""))
      // Conversion d'une colonne texte en minuscules
     .withColumn("nature_juridique_du_rappel", lower(col("nature_juridique_du_rappel")))
  }
}
