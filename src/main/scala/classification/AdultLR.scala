package classification

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{SQLContext, SparkSession}


/**
  * Created by yang on 17-7-2.
  */
object AdultLR {
  val trainPath = "/home/yang/JavaProject/SparkLearning/data/adult_train.csv"
  val testPath = "/home/yang/JavaProject/SparkLearning/data/adult_test.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master( "local[*]")
      .getOrCreate()
    dataPrepare(spark,trainPath)
  }

  def dataPrepare(spark: SparkSession,filePath:String): Unit = {

    import spark.implicits._
    val df = spark.sqlContext.read.format("com.databricks.spark.csv").option("header","true").load(filePath)
    df.printSchema()
    df.show(10)
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()
    // 设置迭代次数并进行进行训练
//    val numIterations = 20
//    val model = SVMWithSGD.train(parsedData, numIterations)
//
//    // 统计分类错误的样本比例
//    val labelAndPreds = parsedData.map { point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//
//    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
//    println("Training Error = " + trainErr)
  }

}





/*
Data schema
>50K, <=50K.

age: continuous.
workclass: Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked.
fnlwgt: continuous.
education: Bachelors, Some-college, 11th, HS-grad, Prof-school, Assoc-acdm, Assoc-voc, 9th, 7th-8th, 12th, Masters, 1st-4th, 10th, Doctorate, 5th-6th, Preschool.
education-num: continuous.
marital-status: Married-civ-spouse, Divorced, Never-married, Separated, Widowed, Married-spouse-absent, Married-AF-spouse.
occupation: Tech-support, Craft-repair, Other-service, Sales, Exec-managerial, Prof-specialty, Handlers-cleaners, Machine-op-inspct, Adm-clerical, Farming-fishing, Transport-moving, Priv-house-serv, Protective-serv, Armed-Forces.
relationship: Wife, Own-child, Husband, Not-in-family, Other-relative, Unmarried.
race: White, Asian-Pac-Islander, Amer-Indian-Eskimo, Other, Black.
sex: Female, Male.
capital-gain: continuous.
capital-loss: continuous.
hours-per-week: continuous.
native-country: United-States, Cambodia, England, Puerto-Rico, Canada, Germany, Outlying-US(Guam-USVI-etc), India, Japan, Greece, South, China, Cuba, Iran, Honduras, Philippines, Italy, Poland, Jamaica, Vietnam, Mexico, Portugal, Ireland, France, Dominican-Republic, Laos, Ecuador, Taiwan, Haiti, Columbia, Hungary, Guatemala, Nicaragua, Scotland, Thailand, Yugoslavia, El-Salvador, Trinadad&Tobago, Peru, Hong, Holand-Netherlands.
 */