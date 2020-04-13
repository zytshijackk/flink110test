package com.streamingkmeans.utils

import scala.collection.mutable.ListBuffer

object BLASTest {


  def main(args: Array[String]): Unit = {
    var l =ListBuffer(1.0,3.0)
    var y =ListBuffer(2.0,4.0)
//    BLAS.scal(3,l)


    BLAS22.axpy(3.0,l,y)
    System.out.println(y)
  }
}
