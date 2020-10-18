package SparkJob

import SparkJob.Domain.SparkParams
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object CsvJob {
    def run(sparkParams: SparkParams) {
        // create sparkSession
        val spark = SparkSession
                    .builder
                    .appName(sparkParams.parser)
                    .getOrCreate()

        // Homework: change the way we use the option to inOption
        val data = spark.read.options(sparkParams.inOptions).csv(sparkParams.inPath)

        // transformation logic below
        val resultDF = data.withColumn("source", lit("wcd"))

        resultDF.write
        .partitionBy(sparkParams.partitionColumn)
        .options(sparkParams.outOptions)
        .format(sparkParams.outFormat)
        .mode(sparkParams.saveMode)
        .save(sparkParams.outPath)
    }


}