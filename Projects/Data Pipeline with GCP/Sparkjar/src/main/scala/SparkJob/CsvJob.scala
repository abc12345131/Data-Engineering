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

        val data = spark.read.options(sparkParams.inOptions).csv(sparkParams.inPath)

        // transformation logic below
        val resultDF = data.withColumn("source", lit("wcd"))

        // write to storage
        resultDF.write
        .partitionBy(sparkParams.partitionColumn)
        .options(sparkParams.outOptions)
        .format(sparkParams.outFormat)
        .mode(sparkParams.saveMode)
        .save(sparkParams.outPath)
    }


}