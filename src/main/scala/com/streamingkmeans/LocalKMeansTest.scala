package com.streamingkmeans

import com.streamingkmeans.utils.StringToSparse
import ml.kmeans.LocalKMeans
import org.apache.flink.api.java.Utils
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.SparseVector
/**
 * @Author: ch
 * @Date: 08/05/2020 9:53 PM
 * @Version 1.0
 * @Describe:
 */
object LocalKMeansTest {
  def main(args: Array[String]): Unit = {
    val input = "/Users/zytshijack/Documents/github/git/myrepositories/flink110test/src/main/resources/file/point.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.readTextFile(input)
    val points = data.map(new StringToSparse)
    val points2= points.collect().toArray
    val initializationSteps = Array.fill(points2.size)(1.0)
    val seed = Utils.RNG.nextLong()
    val kMeansPlusPlus: Array[SparseVector] = LocalKMeans.kMeansPlusPlus(seed, points2, initializationSteps, 2, 2)
    kMeansPlusPlus.foreach(println)
  }
}
