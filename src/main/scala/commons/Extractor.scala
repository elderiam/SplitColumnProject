package commons

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, split}

class Extractor {

  val spark: SparkSession = SparkSession.builder()
    .appName("Split Column App")
    .config("spark.master", "local")
    .getOrCreate()

  def readCSV(inputPath: String, delimiter : String): DataFrame = {
    val originalDf: DataFrame = spark
      .read
      .option("header", "true")
      //.option("ignoreLeadingWhiteSpace", value = true) // escape space before your data
      .option("delimiter", delimiter)
      .csv(inputPath)
      .withColumn("Index", monotonically_increasing_id)

    val firstCol = originalDf.columns(0)
    val secondCol = originalDf.columns(1)
    val dfRenamed = originalDf
      .withColumnRenamed(firstCol, "field1")
      .withColumnRenamed(secondCol, "cutoff_date")
    dfRenamed
  }

  def splitColumn(dataFrame: DataFrame, delimiter: String): DataFrame = {

    val firstCol = dataFrame.columns(0)
    val colNames = dataFrame.select(firstCol).first.getString(0)
    val trimmedList: List[String] = colNames.split("\\"+delimiter).map(_.trim).toList

    val splitDF = dataFrame
      .withColumn("tmp", split(col(firstCol), "\\"+delimiter))
      .select(trimmedList.indices.map(i => col("tmp").getItem(i).as(trimmedList(i))): _*
      )
      .withColumn("Index", monotonically_increasing_id)
    splitDF
  }

  def joinDataFrames(originalDF: DataFrame, splitDF : DataFrame): Unit = {
    val df = originalDF.as("df1").join(splitDF.as("df2"), originalDF("Index") === splitDF("Index"))
      .select("df2.*", "df1.cutoff_date")
      .filter("Index > 0")
      .drop("Index")
    df.show(10, truncate = false)
  }

}
