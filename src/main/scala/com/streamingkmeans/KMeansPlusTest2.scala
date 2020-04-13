package com.streamingkmeans

import com.streamingkmeans.utils.StringToDense
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object KMeansPlusTest2 {
  def main(args: Array[String]): Unit = {
    val input = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/point.txt"
    val envStream = StreamExecutionEnvironment.getExecutionEnvironment
    val kMeansPlus = new KMeansPlus(2,null)
    val dataStream:DataStream[String] = envStream.readTextFile(input)
    val predictStream = dataStream.map(new StringToDense)
    val arr1 = DenseVector(Array[Double](1,1))
    val arr2 = DenseVector(Array[Double](-1,-1))
    val arr = Array[DenseVector](arr1,arr2)
    kMeansPlus.predict(predictStream,arr).print("!!")
    envStream.execute()
  }
}
