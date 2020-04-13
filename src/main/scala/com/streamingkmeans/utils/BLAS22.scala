package com.streamingkmeans.utils

import scala.collection.mutable.ListBuffer

object BLAS22 extends Serializable{
  /**
   * x = a * x
   */
  def scal(a: Double, x: ListBuffer[Double]): Unit = {
    for(i <- 0 to x.size-1){
      x(i)*=a
    }
  }
  /**
   * y += a * x
   */
  def axpy(a: Double, x: ListBuffer[Double], y: ListBuffer[Double]): Unit = {
    for(i <- 0 to y.size-1){
      y(i) += a*x(i)
    }
  }
}
