/**
  * Created by yangqiao on 15/6/17.
  */
import org.apache.spark._
import org.apache.spark.streaming._


//spark streaming demo, using nc -lkv localhost 9999 to generate data
object StreamingDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("streaming").setMaster("local[*]")
    val spark = new SparkContext(conf)
    val ssc = new StreamingContext(spark,Seconds(45))

    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word =>(word,1))
    val wordCount = pairs.reduceByKey(_+_)
    wordCount.saveAsTextFiles("/home/yangqiao/scalaProject/sparkTest/tes")
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
