package joboperation

import commons.Extractor
import org.apache.log4j.{LogManager, Logger}

object SplitColumnOperator {

  val extractor: Extractor = new Extractor
  val LOGGER: Logger = LogManager
    .getLogger("SantanderTest")

  def main(args: Array[String]): Unit = {

    val pathInput = args(0)
    val delimiter = args(1)
    //val pathInput = "src/main/resources/data/datasetWithPipe.csv"
    //val pathInput = "src/main/resources/data/query-hive-84190.csv"
    LOGGER.info(s"Read Dataset from $pathInput with the next delimiter $delimiter")
    val originalDF = extractor.readCSV(pathInput, delimiter)
    LOGGER.info(s"Splitting Columns from $originalDF")
    val splitDF = extractor.splitColumn(originalDF , delimiter)
    LOGGER.info(s"Doing a join with $originalDF and $splitDF")
    extractor.joinDataFrames(originalDF,splitDF)
    LOGGER.info(s"Stopping Spark")
    extractor.spark.stop()
  }
}
