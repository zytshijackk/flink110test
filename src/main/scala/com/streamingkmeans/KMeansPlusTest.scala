package com.streamingkmeans

import com.streamingkmeans.utils.StringToDense
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.tools.util.PathResolver.Environment

object KMeansPlusTest {
  def main(args: Array[String]): Unit = {
//    val arr1 = DenseVector(Array[Double](1,1))
//    val arr2 = DenseVector(Array[Double](-1,-1))
//    val arr3 = DenseVector(Array[Double](0,0))
//    val arr = Array[DenseVector](arr1,arr2,arr3)
//    val kmeansPlus = new KMeansPlus(3,10,arr)
    val input = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/point.txt"
    val centerInput = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/center.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val envStream = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)
    val center = env.readTextFile(centerInput)
    val data:DataSet[DenseVector] = text.map(new StringToDense)
    val dataCenter:DataSet[DenseVector] = center.map(new StringToDense)
    val kMeansPlus = new KMeansPlus(2,dataCenter)
    kMeansPlus.train(data)
    val dataStream:DataStream[String] = envStream.readTextFile(input)
    val predictStream = dataStream.map(new StringToDense)
    data.print()
    val arr1 = DenseVector(Array[Double](1,1))
    val arr2 = DenseVector(Array[Double](-1,-1))
    val arr = Array[DenseVector](arr1,arr2)

    kMeansPlus.predict(predictStream,arr).print("!!")
//    env.execute()
    envStream.execute()
  }
}
