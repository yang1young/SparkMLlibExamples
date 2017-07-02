package MLlibBasic

import breeze.linalg.{DenseMatrix, DenseVector, diag, inv}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yang on 17-7-2.
  */
object Breeze {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("my").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    //visit(sc)
  }
  /*
  def create(sc: SparkContext): Unit = {
    //basic create
    val m1 = DenseMatrix.zeros[Double](2,3)
    val m1_ = DenseMatrix((1,2),(3,4))
    val v1 = DenseVector.range(1,10,2)
    val v2  = DenseVector.ones(3)
    val v2_ = DenseVector.rand(4)

    //unit matrix
    val m2 = DenseMatrix.eye[Double](3)

    //diag matrix
    val m3 = diag(DenseVector(1,2,3))

    //transport
    val m4 = m3.t

    //operate
    val m5 = DenseVector.tabulate(3){i=>i*2}
    val m6 = DenseMatrix.tabulate(3,2){case(i,j)=>i+j}

  }

  def visit(sc: SparkContext): Unit = {
    val a = DenseVector(1,2,3,4,5,6,7,8,9)
    print(a(0))
    a(1 to 4)
    //last item
    a(-1)

    val m = DenseMatrix((1,2,3),(3,4,5))
    //specific item
    m(0,1)
    //specific column
    m(::,1)
  }

  def operate(sc: SparkContext): Unit = {
    val m =  DenseMatrix((1,2,3),(3,4,5))
    m.reshape(3,2)
    m.toDenseVector //to vector
    m.copy
    m(::,2) :=5

    val a = DenseVector(1,2,3,4)
    a(1 to 4) = 5

    //compute
    //+
    //:*<==
    //argmax(a)
    //sum(a)
    //trace(a) only item dial
    //sum(a,Axis._1)  sum on some axis

    m.rows
    m.cols
    inv(m)
  }
*/
}
