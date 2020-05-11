package com.streamingkmeans.utils

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.ml.math.{DenseVector, SparseVector}


class StringToSparse extends MapFunction[String, SparseVector] with Serializable {

  override def map(s: String): SparseVector = {
    val arr:Array[Double] = s.split(" ").map(x => x.toDouble)
    DenseVector(arr).toSparseVector
  }
}