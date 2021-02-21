package org.apache.spark.example

import org.apache.spark.ml.linalg.{BLAS, DenseVector}

/**
  * Created by yihaibo on 18/10/30.
  */

object AppTest {
  def main(args: Array[String]) : Unit = {
    val w = 1.0
    val f = new DenseVector(Array(1,2,3,4))
    val aSum = new DenseVector(Array.ofDim(4))

    BLAS.axpy(w, f, aSum)
    println(aSum)

    val k = f.size
    val triK = k * (k + 1) / 2
    val aaSum = new DenseVector(Array.ofDim(triK))

    BLAS.spr(w, f, aaSum)

    println(aaSum)

  }
}

