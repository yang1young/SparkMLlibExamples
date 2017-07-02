import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * Created by yangqiao on 14/6/17.
  */

object SparkRDDBasic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("my").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    rddTransform(sc)
  }

  def p(rdd: org.apache.spark.rdd.RDD[_]): Unit = rdd.foreach(println) // print

  /*RDD create*/
  def createRDD(sc: SparkContext): Unit = {

    //1.from array
    val data = Array(1, 2, 3, 4)
    val rdd1 = sc.parallelize(data)
    val rdd2 = sc.parallelize(List(1, 2, 3), 2)
    //2.from file
    val rdd3 = sc.textFile("mypath")
    val rdd4 = sc.textFile("hdfs://192.168.1.100:9000/input/data.txt")
    val rdd5 = sc.textFile("file:/input/data.txt")
  }

  def rddTransform(sc: SparkContext): Unit = {

    /*RDD transform*/
    //1.map
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.map(x => x * 2)

    //2.filter
    val rdd3 = rdd2.filter(x => x > 10)

    //3.flatMap:
    val rdd4 = rdd3.flatMap(x => x to 20)

    //4.mapPartition
    //map for data from different partition

    //5.sample, withRepalcement decide whether put back
    val a = sc.parallelize(1 to 1000, 3)
    a.sample(false, 0.1, 0).count()

    //6.union
    val rdd8 = rdd1.union(rdd3)

    //7.intersection
    val rdd9 = rdd8.intersection(rdd1)

    //8.distinct
    val rdd10 = rdd8.distinct()

    //10.goupByKey,input [K,V], return (K,Seq[V])
    val rdd0 = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)), 3)
    val rdd11 = rdd0.groupByKey()
    p(rdd11)

    //11.reduceByKey: input/return [K,V]
    val rdd12 = rdd0.reduceByKey((x, y) => x + y)
    p(rdd12)

    //12.aggregateByKey
    /*
    aggregateByKey(zeroValue,seq,comb,taskNums)
    在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，
    进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给
    combine函数进行计算（先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推）
    ，将key与计算结果作为一个新的kv对输出。
     */
    val data = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3)))
    val result = data.aggregateByKey(5)(math.max, _ + _)
    p(result)

    //13.combineByKey

    //14.sortByKey
    val rdd14 = rdd0.sortByKey()

    //15.join (K,V) and (K,W) =>(K,(V,W))
    val rdd15 = rdd0.join(rdd0)
    //other leftOuterJoin,rightOuterJoin,fullOuterJoin

    //16.cogroup (K,V) and (K,W) =>(K,Seq[V],Seq[W])
    val rdd16 = rdd0.cogroup(rdd0)

    //17.cartesion  T,U=>(T,U)
    val rdd17 = rdd1.cartesian(rdd3)

    //18.pipe operate RDD according to shell command
    val rdd18 = sc.parallelize(1 to 9, 3)
    rdd18.pipe("head -n 1")

    //19.randomSplit
    val rdd19 = rdd1.randomSplit(Array(0.3, 0.7), 1)
    rdd19(0).collect()
    rdd19(1).collect()

    //20. subtract
    val rdd20 = sc.parallelize(1 to 9, 3)
    val rdd20_ = sc.parallelize(1 to 3, 3)
    val rdd20__ = rdd20.subtract(rdd20_)

    //21.zip
    val rdd21 = sc.parallelize(Array(1, 2, 3, 4), 3)
    val rdd21_ = sc.parallelize(Array("a", "b", "c", "d", 3))
    val rdd21__ = rdd21.zip(rdd21_)

    //22.coalesce repartion without shuffle
    //23.repartition repartition with shuffle
    //24.treeAggregate

  }

  def rddAction(sc: SparkContext): Unit = {
      //1. reduce
    val rdd1 = sc.parallelize(1 to 9,3)
    val rdd2 = rdd1.reduce(_+_)

    //2.collect return element as array
    rdd1.collect()

    //3.count
    rdd1.count()

    //4.first :as take(1)
    rdd1.first()

    //5.take
    rdd1.take(1)

    //6.takeSample:ramdom sample and output
    rdd1.takeSample(true,4)

    //7.takeOrdered random n elem and output ordered
    rdd1.takeOrdered(4)

    //8.saveAsTextFile
    rdd1.saveAsTextFile("path")

    //9.countByKey
    //10.foreach(func)


  }


}


/*
 val rddFile = sc.textFile("/home/yang/JavaProject/SparkLearning/data/testFile.csv")

      val rddMap = rdd.map(x=>x*x)
      val rddFilter = rdd.filter(_%2==0)
      val rddFlatMap = rdd.flatMap(x=>1 to x)

      val toLocal = rdd.collect()
      val takeFirst = rdd.take(1)
      val count = rdd.count()
      val reduce = rdd.reduce(_+_)
      rdd.saveAsTextFile("/home/yang/JavaProject/SparkLearning/data")

      val pets = sc.parallelize(List(("dog",1),("cat",1),("dog",2)))
      val reduceByKey = pets.reduceByKey(_+_)
      val groupByKey = pets.groupByKey()
      val sortByKey = pets.sortByKey()
 */