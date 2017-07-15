package classification

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Created by yang on 17-7-2.
  * https://benfradet.github.io/blog/2015/12/16/Exploring-spark.ml-with-the-Titanic-Kaggle-competition
  */
object AdultLR {
  val trainPath = "/home/yang/JavaProject/SparkMLlibExamples/src/main/resources/data/adult_train.csv"
  val testPath = "/home/yang/JavaProject/SparkMLlibExamples/src/main/resources/data/adult_test.csv"

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
    val customSchema = StructType(Array(
      StructField("age", IntegerType, true),
      StructField("workclass", StringType, true),
      StructField("fnlwgt", StringType, true),
      StructField("education", StringType, true),
      StructField("education-num", IntegerType, true),
      StructField("marital-status", StringType, true),
      StructField("occupation", StringType, true),
      StructField("relationship", StringType, true),
      StructField("race", StringType, true),
      StructField("sex", StringType, true),
      StructField("capital-gain", IntegerType, true),
      StructField("capital-loss", IntegerType, true),
      StructField("hours-per-week", IntegerType, true),
      StructField("native-country", StringType, true),
      StructField("label", StringType, true)
    ))

    val originDF = spark.sqlContext.read.format("com.databricks.spark.csv").option("header","true").schema(customSchema)
      .load(filePath)
    val dropDF = originDF.drop(originDF.col("fnlwgt"))
    val trainDF = dropDF.na.fill("0")
    trainDF.printSchema()
    val cols = trainDF.columns.map(trainDF(_)).reverse
    val reversedDF = trainDF.select(cols:_*)
    reversedDF.show(100)


    val categoricalFeatColNames = Seq("native-country","sex","race","relationship","occupation","marital-status","education","workclass")
    val stringIndexers = categoricalFeatColNames.map { colName =>
      new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(colName + "Indexed")
        .fit(reversedDF)
    }

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndexed")
      .fit(reversedDF)


    val numericFeatColNames = Seq("hours-per-week","capital-loss","capital-gain", "education-num","age")

//    val doubleDF = reversedDF.select(numericFeatColNames.map(
//      c => col(c).cast("double")): _*)
//    doubleDF.printSchema()
//    val stringDF = reversedDF.select(categoricalFeatColNames.map(c => col(c)): _*)
//    stringDF.printSchema()


    val idxdCategoricalFeatColName = categoricalFeatColNames.map(_ + "Indexed")
    val allIdxdFeatColNames = numericFeatColNames ++ idxdCategoricalFeatColName
    val assembler = new VectorAssembler()
      .setInputCols(Array(allIdxdFeatColNames: _*))
      .setOutputCol("Features")

    val randomForest = new RandomForestClassifier()
      .setLabelCol("labelIndexed")
      .setFeaturesCol("Features")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val piplineArray = stringIndexers.toArray++Array(labelIndexer, assembler, randomForest, labelConverter)
    val pipeline = new Pipeline().setStages(piplineArray)

    val paramGrid = new ParamGridBuilder()
      .addGrid(randomForest.maxBins, Array(100, 108, 112))
      .addGrid(randomForest.maxDepth, Array(4, 6, 8))
      .addGrid(randomForest.impurity, Array("entropy", "gini"))
      .build()

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("labelIndexed")
      .setMetricName("areaUnderPR")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val crossValidatorModel = cv.fit(reversedDF)
    val predictions = crossValidatorModel.transform(reversedDF)


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

 >50K, <=50K.
StructField("A", IntegerType()),
    StructField("B", DoubleType()),
    StructField("C", StringType()),
StructField("age",IntegerType()),
StructField("workclass", StringType()),
StructField("fnlwgt", DoubleType()),
StructField("education", StringType()),
StructField("education-num",IntegerType()
StructField("marital-status", StringType()),
StructField("occupation", StringType()),
StructField("relationship", StringType()),
StructField("race", StringType()),
StructField("sex", StringType()),
StructField("capital-gain", DoubleType()),
StructField("capital-loss", DoubleType()),
StructField("hours-per-week", DoubleType()),
StructField("native-country", StringType()),
StructField("label", StringType())
*/