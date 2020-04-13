package com.streamingkmeans.utils

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.ml.math.{DenseVector, SparseVector}


class StringToDense extends MapFunction[String, DenseVector] with Serializable {

  override def map(s: String): DenseVector = {
    val arr:Array[Double] = s.split(",").map(x => x.toDouble)
    DenseVector(arr)
  }
}