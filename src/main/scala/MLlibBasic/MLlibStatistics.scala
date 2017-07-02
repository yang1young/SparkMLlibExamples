package MLlibBasic

import breeze.linalg
import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by yang on 17-7-2.
  */
object MLlibStatistics {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("my").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    columnsStatics(sc)
  }

  def columnsStatics(sc: SparkContext): Unit = {
    val dataPath = "/home/yang/JavaProject/SparkLearning/data/test.csv"
    val data = sc.textFile(dataPath).map(_.split("\t")).map(f=>f.map(f=>f.toDouble))
    val dv: DenseVector[Double] = linalg.DenseVector(1.0, 0.0, 3.0)
    val data1 = data.map(f=>org.apache.spark.mllib.linalg.Vectors.dense(f))
    val stat1 = Statistics.colStats(data1)
    //some basic statics of columns
    println(stat1.max)
    println(stat1.min)
    println(stat1.normL1)
    println(stat1.normL2)
    println(stat1.variance)

    //pearsonCorr
    val corr1 = Statistics.corr(data1,"pearson")
    //spearman
    val corr2 = Statistics.corr(data1,"spearman")

    //chis-squared check
    val v1 = Vectors.dense(43,9)
    val v2 = Vectors.dense(44,4.0)
    val c1 = Statistics.chiSqTest(v1,v2)
  }


}
