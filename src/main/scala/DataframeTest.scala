import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object DataframeTest {

  case class DataSample(code:String,value:Long)

  def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .master( "local[*]")
        .getOrCreate()

      runDatasetCreationExample(spark)
      spark.stop()
    }


    private def runDatasetCreationExample(spark: SparkSession): Unit = {
      import spark.implicits._
      val rddWithSchema = Seq(DataSample("AA",150000),DataSample("BB",80000)).toDF()
      rddWithSchema.show()

      rddWithSchema.createOrReplaceTempView("newTable")
      spark.sqlContext.sql("SHOW TABLES").show()

      spark.sqlContext.sql("CREATE TABLE table1 AS SELECT * FROM newTable")
      spark.sqlContext.sql("SHOW TABLES").show()


    }



}
