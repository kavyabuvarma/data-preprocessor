import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PreProcessor{

  def anonymizeData(spark: SparkSession, logFile: String) {

    val df_original = spark.read.option("header", true).csv(logFile)
    df_original.printSchema()
    df_original.show(10, false)

    val df_anonymized = df_original.select(col("date_of_birth"),
      lit("XXXXXX").as("first_name"),
      lit("YYYYYY").as("last_name"),
      regexp_replace(df_original.col("address"), "(^[A-Z]+ [A-Z]+)",  "ZZZZZZZZZZ").as("address"),
    )

    df_anonymized.printSchema()
    df_anonymized.show(10, false)


  }

  def main(args: Array[String]) {

    var logFile = "data_1mb.csv"

    if (args.length > 0 ){
      logFile = args(0)
    }

    val spark = SparkSession.builder.appName("Preprocessor").getOrCreate()

    anonymizeData(spark, logFile)

    spark.stop()
  }
}