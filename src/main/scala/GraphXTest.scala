/**
  * Created by yangqiao on 16/6/17.
  */
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object GraphXTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master( "local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    val vertices = Array((1L,"SFO"),(2L,"ORD"),(1L,"DFW"))
    val vRDD = sc.parallelize(vertices)
    val edges = Array(Edge(1L,2L,1800),Edge(2L,3L,800),Edge(3L,1L,1400))
    val eRDD = sc.parallelize(edges)
    val nowhere = "nowhere"
    val graph = Graph(vRDD,eRDD,nowhere)

    graph.vertices.collect.foreach(println)
    graph.edges.collect.foreach(println)


  }

}
