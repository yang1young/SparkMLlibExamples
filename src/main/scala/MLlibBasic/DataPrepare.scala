package MLlibBasic

import breeze.optimize.proximal.LogisticGenerator
import org.apache.spark.mllib.util.{KMeansDataGenerator, LinearDataGenerator, LogisticRegressionDataGenerator, MLUtils}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yang on 17-7-2.
  */
object DataPrepare {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("my").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
  }

  def libsvmData(sc: SparkContext): Unit = {
    //1.load
    val data = MLUtils.loadLibSVMFile(sc,"path")
    //2.save
    MLUtils.saveAsLibSVMFile(data,"path")
  }

  def dataGenerate(sc: SparkContext): Unit = {
    //1.appendBias: add a bias value
    //2.fastSquaredDistance  fast vector distance compute
    //3. generateKMeansRDD: numPoints, k:cluster num, d: dimention,r scale factor
    val kmeansRDD = KMeansDataGenerator.generateKMeansRDD(sc,40,5,3,1.0,2)

    //4.generateLinearRDD linear regression data generate
    val linearRDD = LinearDataGenerator.generateLinearRDD(sc,40,3,1,2,0.0)

    //5.logisticRegression data
    val logisticRDD = LogisticRegressionDataGenerator.generateLogisticRDD(sc,40,3,1,2,0.5)

  }
}
